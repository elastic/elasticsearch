/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.repositories.blobstore;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexFormatTooNewException;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.InputStreamDataInput;
import org.apache.lucene.store.OutputStreamIndexOutput;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.CheckedBiFunction;
import org.elasticsearch.common.Numbers;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.lucene.store.IndexOutputOutputStream;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.gateway.CorruptStateException;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.zip.CRC32;

/**
 * Snapshot metadata file format used in v2.0 and above
 */
public final class ChecksumBlobStoreFormat<T> {

    // Serialization parameters to specify correct context for metadata serialization.
    // When metadata is serialized certain elements of the metadata shouldn't be included into snapshot
    // exclusion of these elements is done by setting Metadata.CONTEXT_MODE_PARAM to Metadata.CONTEXT_MODE_SNAPSHOT
    public static final ToXContent.Params SNAPSHOT_ONLY_FORMAT_PARAMS = new ToXContent.MapParams(
        Collections.singletonMap(Metadata.CONTEXT_MODE_PARAM, Metadata.CONTEXT_MODE_SNAPSHOT)
    );

    // The format version
    public static final int VERSION = 1;

    private static final int BUFFER_SIZE = 4096;

    private final String codec;

    private final String blobNameFormat;

    private final CheckedBiFunction<String, XContentParser, T, IOException> reader;

    private final CheckedBiFunction<String, XContentParser, T, IOException> fallbackReader;

    private final Function<T, ? extends ToXContent> writer;

    /**
     * @param codec          codec name
     * @param blobNameFormat format of the blobname in {@link String#format} format
     * @param reader         prototype object that can deserialize T from XContent
     * @param fallbackReader fallback prototype object that can deserialize T from XContent in case reader fails
     * @param writer         function that maps the given type to a {@link ToXContent} for serialization
     */
    public ChecksumBlobStoreFormat(
        String codec,
        String blobNameFormat,
        CheckedBiFunction<String, XContentParser, T, IOException> reader,
        @Nullable CheckedBiFunction<String, XContentParser, T, IOException> fallbackReader,
        Function<T, ? extends ToXContent> writer
    ) {
        this.reader = reader;
        this.blobNameFormat = blobNameFormat;
        this.codec = codec;
        this.fallbackReader = fallbackReader;
        this.writer = writer;
    }

    /**
     * @param codec          codec name
     * @param blobNameFormat format of the blobname in {@link String#format} format
     * @param reader         prototype object that can deserialize T from XContent
     * @param writer         function that maps the given type to a {@link ToXContent} for serialization
     */
    public ChecksumBlobStoreFormat(
        String codec,
        String blobNameFormat,
        CheckedBiFunction<String, XContentParser, T, IOException> reader,
        Function<T, ? extends ToXContent> writer
    ) {
        this(codec, blobNameFormat, reader, null, writer);
    }

    /**
     * Reads and parses the blob with given name, applying name translation using the {link #blobName} method
     *
     * @param blobContainer blob container
     * @param name          name to be translated into
     * @return parsed blob object
     */
    public T read(String repoName, BlobContainer blobContainer, String name, NamedXContentRegistry namedXContentRegistry)
        throws IOException {
        String blobName = blobName(name);
        try (InputStream in = blobContainer.readBlob(OperationPurpose.SNAPSHOT_METADATA, blobName)) {
            return deserialize(repoName, namedXContentRegistry, in);
        }
    }

    public String blobName(String name) {
        return String.format(Locale.ROOT, blobNameFormat, name);
    }

    public T deserialize(String repoName, NamedXContentRegistry namedXContentRegistry, InputStream input) throws IOException {
        final DeserializeMetaBlobInputStream deserializeMetaBlobInputStream = new DeserializeMetaBlobInputStream(input);
        try {
            CodecUtil.checkHeader(new InputStreamDataInput(deserializeMetaBlobInputStream), codec, VERSION, VERSION);
            final InputStream wrappedStream;
            if (deserializeMetaBlobInputStream.nextBytesCompressed()) {
                wrappedStream = CompressorFactory.COMPRESSOR.threadLocalInputStream(deserializeMetaBlobInputStream);
            } else {
                wrappedStream = deserializeMetaBlobInputStream;
            }
            T result;

            if (fallbackReader != null) {
                // fully read stream to allow rereading it when calling fallback
                BytesReference bytesReference = Streams.readFully(wrappedStream);
                deserializeMetaBlobInputStream.verifyFooter();
                try (
                    XContentParser parser = XContentHelper.createParserNotCompressed(
                        XContentParserConfiguration.EMPTY.withRegistry(namedXContentRegistry)
                            .withDeprecationHandler(LoggingDeprecationHandler.INSTANCE),
                        bytesReference,
                        XContentType.SMILE
                    )
                ) {
                    result = reader.apply(repoName, parser);
                    XContentParserUtils.ensureExpectedToken(null, parser.nextToken(), parser);
                } catch (Exception e) {
                    try (
                        XContentParser parser = XContentHelper.createParserNotCompressed(
                            XContentParserConfiguration.EMPTY.withRegistry(namedXContentRegistry)
                                .withDeprecationHandler(LoggingDeprecationHandler.INSTANCE),
                            bytesReference,
                            XContentType.SMILE
                        )
                    ) {
                        result = fallbackReader.apply(repoName, parser);
                        XContentParserUtils.ensureExpectedToken(null, parser.nextToken(), parser);
                    }
                }
            } else {
                try (
                    XContentParser parser = XContentType.SMILE.xContent()
                        .createParser(namedXContentRegistry, LoggingDeprecationHandler.INSTANCE, wrappedStream)
                ) {
                    result = reader.apply(repoName, parser);
                    XContentParserUtils.ensureExpectedToken(null, parser.nextToken(), parser);
                }
                deserializeMetaBlobInputStream.verifyFooter();
            }
            return result;
        } catch (CorruptIndexException | IndexFormatTooOldException | IndexFormatTooNewException ex) {
            // we trick this into a dedicated exception with the original stacktrace
            throw new CorruptStateException(ex);
        } catch (Exception e) {
            try {
                // drain stream fully and check whether the footer is corrupted
                Streams.consumeFully(deserializeMetaBlobInputStream);
                deserializeMetaBlobInputStream.verifyFooter();
            } catch (CorruptStateException cse) {
                cse.addSuppressed(e);
                throw cse;
            } catch (Exception ex) {
                e.addSuppressed(ex);
            }
            throw e;
        }
    }

    /**
     * Wrapper input stream for deserializing blobs that come with a Lucene header and footer in a streaming manner. It manually manages
     * a read buffer to enable not reading into the last 16 bytes (the footer length) of the buffer via the standard read methods so that
     * a parser backed by this stream will only see the blob's body.
     */
    private static final class DeserializeMetaBlobInputStream extends FilterInputStream {

        // checksum updated with all but the last 8 bytes read from the wrapped stream
        private final CRC32 crc32 = new CRC32();

        // Only the first buffer.length - 16 bytes are exposed by the read() methods; once the read position reaches 16 bytes from the end
        // of the buffer the remaining 16 bytes are moved to the start of the buffer and the rest of the buffer is filled from the stream.
        private final byte[] buffer = new byte[1024 * 8];

        // the number of bytes in the buffer, in [0, buffer.length], equal to buffer.length unless the last fill hit EOF
        private int bufferCount;

        // the current read position within the buffer, in [0, bufferCount - 16]
        private int bufferPos;

        DeserializeMetaBlobInputStream(InputStream in) {
            super(in);
        }

        @Override
        public int read() throws IOException {
            if (getAvailable() <= 0) {
                return -1;
            }
            return buffer[bufferPos++];
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            int remaining = len;
            int read = 0;
            while (remaining > 0) {
                final int r = doRead(b, off + read, remaining);
                if (r <= 0) {
                    break;
                }
                read += r;
                remaining -= r;
            }
            if (len > 0 && remaining == len) {
                // nothing to read, EOF
                return -1;
            }
            return read;
        }

        @Override
        public void close() throws IOException {
            // not closing the wrapped stream
        }

        private int doRead(byte[] b, int off, int len) throws IOException {
            final int available = getAvailable();
            if (available < 0) {
                return -1;
            }
            final int read = Math.min(available, len);
            System.arraycopy(buffer, bufferPos, b, off, read);
            bufferPos += read;
            return read;
        }

        /**
         * Verify footer of the bytes read by this stream the same way {@link CodecUtil#checkFooter(ChecksumIndexInput)} would.
         *
         * @throws CorruptStateException if footer is found to be corrupted
         */
        void verifyFooter() throws CorruptStateException {
            if (bufferCount - bufferPos != CodecUtil.footerLength()) {
                throw new CorruptStateException(
                    "should have consumed all but 16 bytes from the buffer but saw buffer pos ["
                        + bufferPos
                        + "] and count ["
                        + bufferCount
                        + "]"
                );
            }
            crc32.update(buffer, 0, bufferPos + 8);
            final int magicFound = Numbers.bytesToInt(buffer, bufferPos);
            if (magicFound != CodecUtil.FOOTER_MAGIC) {
                throw new CorruptStateException("unexpected footer magic [" + magicFound + "]");
            }
            final int algorithmFound = Numbers.bytesToInt(buffer, bufferPos + 4);
            if (algorithmFound != 0) {
                throw new CorruptStateException("unexpected algorithm [" + algorithmFound + "]");
            }
            final long checksum = crc32.getValue();
            final long checksumInFooter = Numbers.bytesToLong(buffer, bufferPos + 8);
            if (checksum != checksumInFooter) {
                throw new CorruptStateException("checksums do not match read [" + checksum + "] but expected [" + checksumInFooter + "]");
            }
        }

        /**
         * @return true if the next bytes in this stream are compressed
         */
        boolean nextBytesCompressed() {
            // we already have bytes buffered here because we verify the blob's header (far less than the 8k buffer size) before calling
            // this method
            assert bufferPos > 0 : "buffer position must be greater than 0 but was [" + bufferPos + "]";
            return CompressorFactory.COMPRESSOR.isCompressed(new BytesArray(buffer, bufferPos, bufferCount - bufferPos));
        }

        /**
         * @return the number of bytes available in the buffer, possibly refilling the buffer if needed
         */
        private int getAvailable() throws IOException {
            final int footerLen = CodecUtil.footerLength();
            if (bufferCount == 0) {
                // first read, fill the buffer
                bufferCount = org.elasticsearch.core.Streams.readFully(in, buffer, 0, buffer.length);
            } else if (bufferPos == bufferCount - footerLen) {
                // crc and discard all but the last 16 bytes in the buffer that might be the footer bytes
                assert bufferCount >= footerLen;
                crc32.update(buffer, 0, bufferPos);
                System.arraycopy(buffer, bufferPos, buffer, 0, footerLen);
                bufferCount = footerLen + org.elasticsearch.core.Streams.readFully(in, buffer, footerLen, buffer.length - footerLen);
                bufferPos = 0;
            }
            // bytes in the buffer minus 16 bytes that could be the footer
            return bufferCount - bufferPos - footerLen;
        }
    }

    /**
     * Writes blob with resolving the blob name using {@link #blobName} method.
     * <p>
     * The blob will optionally be compressed.
     *
     * @param obj                 object to be serialized
     * @param blobContainer       blob container
     * @param name                blob name
     * @param compress            whether to use compression
     */
    public void write(T obj, BlobContainer blobContainer, String name, boolean compress) throws IOException {
        write(obj, blobContainer, name, compress, Collections.emptyMap());
    }

    /**
     * Writes blob with resolving the blob name using {@link #blobName} method.
     * <p>
     * The blob will optionally be compressed.
     *
     * @param obj                            object to be serialized
     * @param blobContainer                  blob container
     * @param name                           blob name
     * @param compress                       whether to use compression
     * @param serializationParams            extra serialization parameters
     */
    public void write(T obj, BlobContainer blobContainer, String name, boolean compress, Map<String, String> serializationParams)
        throws IOException {
        final String blobName = blobName(name);
        blobContainer.writeMetadataBlob(
            OperationPurpose.SNAPSHOT_METADATA,
            blobName,
            false,
            false,
            out -> serialize(obj, blobName, compress, serializationParams, out)
        );
    }

    public void serialize(final T obj, final String blobName, final boolean compress, final OutputStream outputStream) throws IOException {
        serialize(obj, blobName, compress, Collections.emptyMap(), outputStream);
    }

    public void serialize(
        final T obj,
        final String blobName,
        final boolean compress,
        final Map<String, String> extraParams,
        final OutputStream outputStream
    ) throws IOException {
        try (
            OutputStreamIndexOutput indexOutput = new OutputStreamIndexOutput(
                "ChecksumBlobStoreFormat.serialize(blob=\"" + blobName + "\")",
                blobName,
                org.elasticsearch.core.Streams.noCloseStream(outputStream),
                BUFFER_SIZE
            )
        ) {
            CodecUtil.writeHeader(indexOutput, codec, VERSION);
            try (OutputStream indexOutputOutputStream = new IndexOutputOutputStream(indexOutput) {
                @Override
                public void close() {
                    // this is important since some of the XContentBuilders write bytes on close.
                    // in order to write the footer we need to prevent closing the actual index input.
                }
            };
                XContentBuilder builder = XContentFactory.contentBuilder(
                    XContentType.SMILE,
                    compress ? CompressorFactory.COMPRESSOR.threadLocalOutputStream(indexOutputOutputStream) : indexOutputOutputStream
                )
            ) {
                ToXContent.Params params = extraParams.isEmpty()
                    ? SNAPSHOT_ONLY_FORMAT_PARAMS
                    : new ToXContent.DelegatingMapParams(extraParams, SNAPSHOT_ONLY_FORMAT_PARAMS);

                builder.startObject();
                writer.apply(obj).toXContent(builder, params);
                builder.endObject();
            }
            CodecUtil.writeFooter(indexOutput);
        }
    }
}
