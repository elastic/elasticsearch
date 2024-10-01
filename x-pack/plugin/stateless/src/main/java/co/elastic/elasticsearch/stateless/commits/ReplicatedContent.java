/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.commits;

import co.elastic.elasticsearch.stateless.cache.Lucene90CompoundEntriesReader;
import co.elastic.elasticsearch.stateless.commits.InternalFilesReplicatedRanges.InternalFileReplicatedRange;
import co.elastic.elasticsearch.stateless.commits.StatelessCompoundCommit.InternalFile;
import co.elastic.elasticsearch.stateless.commits.VirtualBatchedCompoundCommit.InternalDataReader;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.elasticsearch.common.lucene.store.InputStreamIndexInput;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.store.LuceneFilesExtensions;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

import static co.elastic.elasticsearch.stateless.commits.InternalFilesReplicatedRanges.REPLICATED_CONTENT_FOOTER_SIZE;
import static co.elastic.elasticsearch.stateless.commits.InternalFilesReplicatedRanges.REPLICATED_CONTENT_HEADER_SIZE;
import static co.elastic.elasticsearch.stateless.commits.InternalFilesReplicatedRanges.REPLICATED_CONTENT_MAX_SINGLE_FILE_SIZE;

/**
 * This class creates a replicated content section that can be later added to the BVCC.
 * Captured data includes both:
 * - internal file replicated ranges (using corresponding readers)
 * - header required to interpret them
 */
class ReplicatedContent {

    private static final Logger logger = LogManager.getLogger(ReplicatedContent.class);

    private static final ReplicatedContent EMPTY = new ReplicatedContent();

    private final List<InternalFileReplicatedRange> ranges = new ArrayList<>();
    private final List<InternalFileRangeReader> readers = new ArrayList<>();

    public static ReplicatedContent create(
        boolean useInternalFilesReplicatedContent,
        Collection<InternalFile> internalFiles,
        Directory directory
    ) {
        if (useInternalFilesReplicatedContent == false) {
            return EMPTY;
        }

        // It is not possible to know the absolute position in CC since header and replicated content can not be materialized yet.
        // So targetContentOffset is the offset of the data after the header and the replicated ranges.
        long targetContentOffset = 0;
        var content = new ReplicatedContent();
        for (var internalFile : internalFiles) {
            if (internalFile.length() <= REPLICATED_CONTENT_MAX_SINGLE_FILE_SIZE) {
                content.append(internalFile.name(), directory, targetContentOffset, 0, (short) internalFile.length(), false);
            } else {
                boolean isCompoundSegmentsFile = LuceneFilesExtensions.fromFile(internalFile.name()) == LuceneFilesExtensions.CFS;
                content.append(
                    internalFile.name(),
                    directory,
                    targetContentOffset,
                    0,
                    REPLICATED_CONTENT_HEADER_SIZE,
                    isCompoundSegmentsFile
                );
                if (isCompoundSegmentsFile) {
                    var entries = readSortedCompoundEntries(directory, correspondingCfeFilename(internalFile.name()));
                    for (var entry : entries) {
                        if (entry.length() <= REPLICATED_CONTENT_MAX_SINGLE_FILE_SIZE) {
                            content.append(
                                internalFile.name(),
                                directory,
                                targetContentOffset,
                                entry.offset(),
                                (short) entry.length(),
                                false
                            );
                        } else {
                            content.append(
                                internalFile.name(),
                                directory,
                                targetContentOffset,
                                entry.offset(),
                                REPLICATED_CONTENT_HEADER_SIZE,
                                false
                            );
                            content.append(
                                internalFile.name(),
                                directory,
                                targetContentOffset,
                                entry.offset() + entry.length() - REPLICATED_CONTENT_FOOTER_SIZE,
                                REPLICATED_CONTENT_FOOTER_SIZE,
                                false
                            );
                        }
                    }
                }
                content.append(
                    internalFile.name(),
                    directory,
                    targetContentOffset,
                    internalFile.length() - REPLICATED_CONTENT_FOOTER_SIZE,
                    REPLICATED_CONTENT_FOOTER_SIZE,
                    false
                );
            }
            targetContentOffset += internalFile.length();
        }

        assert content.assertContentLength();
        return content;
    }

    private boolean assertContentLength() {
        var declaredContentLength = ranges.stream().mapToLong(InternalFileReplicatedRange::length).sum();
        var copiedContentLength = readers.stream().mapToLong(InternalFileRangeReader::rangeLength).sum();
        assert declaredContentLength == copiedContentLength;
        return true;
    }

    private static String correspondingCfeFilename(String cfsFilename) {
        assert LuceneFilesExtensions.fromFile(cfsFilename) == LuceneFilesExtensions.CFS;
        return cfsFilename.substring(0, cfsFilename.length() - 3) + LuceneFilesExtensions.CFE.getExtension();
    }

    private static List<Lucene90CompoundEntriesReader.FileEntry> readSortedCompoundEntries(Directory directory, String filename) {
        try {
            var entries = Lucene90CompoundEntriesReader.readEntries(directory, filename).values();
            return entries.stream().sorted(Comparator.comparingLong(Lucene90CompoundEntriesReader.FileEntry::offset)).toList();
        } catch (IOException e) {
            logger.warn(() -> "Failed to parse [" + filename + "] entries", e);
            assert false;
            return List.of();
        }
    }

    private void append(
        String filename,
        Directory directory,
        long internalFileOffset,
        long fileOffset,
        short length,
        boolean forceNewRange
    ) {
        appendRange(internalFileOffset + fileOffset, length, forceNewRange);
        appendReader(filename, directory, fileOffset, length);
    }

    private void appendRange(long blobContentOffset, short length, boolean forceNewRange) {
        assert ranges.isEmpty() || ranges.getLast().position() <= blobContentOffset;
        if (forceNewRange
            || ranges.isEmpty()
            || ranges.getLast().position() + ranges.getLast().length() < blobContentOffset
            || computeMergedLength(ranges.getLast(), blobContentOffset, length) >= Short.MAX_VALUE) {
            assert ranges.isEmpty() || ranges.getLast().position() + ranges.getLast().length() <= blobContentOffset;
            ranges.add(new InternalFileReplicatedRange(blobContentOffset, length));
        } else {
            var last = ranges.removeLast();
            ranges.add(
                new InternalFileReplicatedRange(
                    last.position(),
                    (short) computeMergedLength(last, blobContentOffset, length) // above condition guards from overflow
                )
            );
        }
    }

    private static long computeMergedLength(InternalFileReplicatedRange range, long otherRangeOffset, short otherRangeLength) {
        return Math.max(range.position() + range.length(), otherRangeOffset + otherRangeLength) - range.position();
    }

    private void appendReader(String filename, Directory directory, long fileOffset, short length) {
        if (readers.isEmpty()
            || Objects.equals(readers.getLast().filename, filename) == false
            || readers.getLast().rangeOffset() + readers.getLast().rangeLength() < fileOffset) {
            readers.add(new InternalFileRangeReader(filename, directory, fileOffset, length));
        } else {
            var last = readers.removeLast();
            readers.add(
                new InternalFileRangeReader(
                    filename,
                    directory,
                    last.rangeOffset(),
                    Math.max(last.rangeOffset() + last.rangeLength(), fileOffset + length) - last.rangeOffset()
                )
            );
        }
    }

    InternalFilesReplicatedRanges header() {
        return InternalFilesReplicatedRanges.from(Collections.unmodifiableList(ranges));
    }

    List<InternalFileRangeReader> readers() {
        return Collections.unmodifiableList(readers);
    }

    /**
     * Internal data range reader for the range of the internal file, that will be replicated in the beginning of a CC
     */
    record InternalFileRangeReader(String filename, Directory directory, long rangeOffset, long rangeLength) implements InternalDataReader {

        @Override
        public InputStream getInputStream(long offset, long length) throws IOException {
            long fileLength = rangeOffset + rangeLength;
            long fileOffset = rangeOffset + offset;
            assert fileOffset < fileLength
                : "offset [" + rangeOffset + "+" + offset + "] more than file length [" + rangeOffset + "+" + rangeLength + "]";
            long fileBytesToRead = Math.min(Math.min(length, rangeLength), fileLength - fileOffset);
            IndexInput input = directory.openInput(filename, IOContext.READ);
            try {
                input.seek(fileOffset);
                return new InputStreamIndexInput(input, fileBytesToRead) {
                    @Override
                    public void close() throws IOException {
                        IOUtils.close(super::close, input);
                    }
                };
            } catch (IOException e) {
                IOUtils.closeWhileHandlingException(input);
                throw e;
            }
        }
    }
}
