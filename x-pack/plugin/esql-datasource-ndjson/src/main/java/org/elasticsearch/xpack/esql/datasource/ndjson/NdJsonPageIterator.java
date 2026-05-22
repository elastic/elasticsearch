/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.ndjson;

import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.util.Check;
import org.elasticsearch.xpack.esql.datasources.spi.ErrorPolicy;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Iterator that reads NDJSON lines and produces ESQL Pages.
 *
 * <p>When {@code skipFirstLine} is true (for non-first splits), discards bytes up to
 * and including the first newline before starting to parse. This implements the standard
 * line-alignment protocol for split boundary handling.
 *
 * <p>When {@code resolvedAttributes} is provided, uses those instead of inferring schema
 * from the split data, avoiding the risk of schema divergence across splits.
 */
final class NdJsonPageIterator implements CloseableIterator<Page> {

    private static final Logger logger = LogManager.getLogger(NdJsonPageIterator.class);

    private final NdJsonPageDecoder pageDecoder;
    private final int rowLimit;
    private long rowsEmitted;
    private boolean endOfFile = false;
    private Page nextPage;

    /**
     * Storage objects up to this size are eagerly slurped into a {@code byte[]} so the decoder can
     * use Jackson's {@code createParser(byte[])} fast path instead of the {@code InputStream}
     * dispatch. Streaming-parallel chunks are ~1 MiB, single-file reads are usually well under
     * this; very large files fall back to streaming so they do not pin a multi-hundred-MiB array.
     */
    static final int BYTE_ARRAY_FAST_PATH_MAX_SIZE = 16 * 1024 * 1024;

    NdJsonPageIterator(
        StorageObject object,
        List<String> projectedColumns,
        int batchSize,
        int rowLimit,
        BlockFactory blockFactory,
        boolean skipFirstLine,
        boolean trimLastPartialLine,
        List<Attribute> resolvedAttributes,
        ErrorPolicy errorPolicy
    ) throws IOException {
        Check.isTrue(errorPolicy != null, "errorPolicy must not be null");
        String sourceLocation = object.path().toString();
        InputStream inputStream = object.newStream();
        if (skipFirstLine) {
            skipToNextLine(inputStream);
        }
        if (trimLastPartialLine) {
            inputStream = trimLastPartialLine(inputStream, errorPolicy, sourceLocation);
        }
        this.rowLimit = rowLimit;
        if (canUseByteArrayFastPath(object)) {
            byte[] data;
            try (InputStream toClose = inputStream) {
                data = toClose.readAllBytes();
            }
            this.pageDecoder = new NdJsonPageDecoder(
                data,
                0,
                data.length,
                resolvedAttributes,
                projectedColumns,
                batchSize,
                blockFactory,
                errorPolicy,
                sourceLocation
            );
        } else {
            this.pageDecoder = new NdJsonPageDecoder(
                inputStream,
                resolvedAttributes,
                projectedColumns,
                batchSize,
                blockFactory,
                errorPolicy,
                sourceLocation
            );
        }
    }

    /**
     * The byte-array fast path is available for storage objects with a known, bounded length;
     * decompressing wrappers throw {@link UnsupportedOperationException} from {@link StorageObject#length()}
     * and stay on the streaming path. Transient {@link IOException}s from {@link StorageObject#length()}
     * also fall back to streaming so a metadata hiccup does not abort an open call; the streaming
     * read will surface the same condition if the data itself is unreachable.
     */
    private static boolean canUseByteArrayFastPath(StorageObject object) {
        try {
            long len = object.length();
            return len >= 0 && len <= BYTE_ARRAY_FAST_PATH_MAX_SIZE;
        } catch (UnsupportedOperationException e) {
            return false;
        } catch (IOException e) {
            // Surface the metadata hiccup at DEBUG so a transient S3 head-object failure is not
            // completely invisible during diagnosis. The streaming path will surface the same
            // condition on read if the data itself is unreachable.
            logger.debug(() -> "byte-array fast path disabled for [" + object.path() + "]: object length unavailable", e);
            return false;
        }
    }

    @Override
    public boolean hasNext() {
        if (nextPage != null) {
            return true;
        }
        if (endOfFile) {
            return false;
        }
        if (rowLimit != FormatReader.NO_LIMIT && rowsEmitted >= rowLimit) {
            endOfFile = true;
            return false;
        }
        try {
            nextPage = pageDecoder.decodePage();
            if (nextPage == null) {
                endOfFile = true;
                return false;
            }
            if (rowLimit != FormatReader.NO_LIMIT) {
                long allowed = (long) rowLimit - rowsEmitted;
                if (allowed <= 0) {
                    nextPage.releaseBlocks();
                    nextPage = null;
                    endOfFile = true;
                    return false;
                }
                int positionCount = nextPage.getPositionCount();
                if (positionCount > allowed) {
                    Page sliced = nextPage.slice(0, (int) allowed);
                    nextPage.releaseBlocks();
                    nextPage = sliced;
                    endOfFile = true;
                }
                rowsEmitted += nextPage.getPositionCount();
            } else {
                rowsEmitted += nextPage.getPositionCount();
            }
            return true;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Page next() {
        if (hasNext() == false) {
            throw new NoSuchElementException();
        }
        Page result = nextPage;
        nextPage = null;
        return result;
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(pageDecoder);
    }

    /**
     * Reader-side half of the split-alignment protocol: drop the leading partial record that the
     * splitter's previous macro-split already emitted via finish-current-line.
     *
     * <p>Protocol cross-references (prose because these live in sibling plugin modules):
     * <ul>
     *   <li>Codec side — {@code Bzip2DecompressionCodec.BlockBoundedDecompressStream} in
     *       the {@code esql-datasource-bzip2} module emits bytes past the split boundary up to
     *       (and including) the next {@code '\n'}.</li>
     *   <li>Splitter side — {@code FileSplitProvider.tryBlockAlignedSplits} in the
     *       {@code esql} module sets the first-split vs. non-first-split markers that
     *       {@link NdJsonFormatReader#read} uses to decide whether to call this method.</li>
     * </ul>
     *
     * <p>Delegates to {@link NdJsonFormatReader#scanForTerminator} so LF/CRLF/CR are handled
     * uniformly; in practice the codec's finish-current-line always ends on {@code '\n'}, so
     * only the LF branch fires, but routing through one implementation removes the coupling.
     */
    static void skipToNextLine(InputStream stream) throws IOException {
        NdJsonFormatReader.scanForTerminator(stream);
    }

    /**
     * Returns a stream that exposes the same bytes as fully reading {@code in} and truncating after
     * the last {@code '\n'}, without materializing the whole stream in memory. The delegate is closed
     * when the returned stream is closed. Oversized partial lines follow {@code errorPolicy}.
     */
    static InputStream trimLastPartialLine(InputStream in, ErrorPolicy errorPolicy, String sourceLocation) {
        // TODO: thread in a centralized error counter?
        return new TrimLastPartialLineInputStream(in, errorPolicy, sourceLocation);
    }
}
