/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.commits;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.ByteArrayDataInput;
import org.elasticsearch.blobcache.BlobCacheUtils;
import org.elasticsearch.blobcache.common.BlobCacheBufferedIndexInput;
import org.elasticsearch.blobcache.common.ByteRange;
import org.elasticsearch.blobcache.shared.SharedBytes;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.store.LuceneFilesExtensions;
import org.elasticsearch.xpack.stateless.cache.Lucene90CompoundEntriesReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * Prototype ("cheap shards", phase 4, default off, see
 * {@link org.elasticsearch.xpack.stateless.recovery.FastSearchActivationSettings#COMMIT_POINTER_OPEN_BYTES_MAX_SIZE_SETTING}): the bytes
 * that opening a Lucene reader on a commit reads, carried in the {@link CommitPointer} so that a search shard activated from the pointer
 * can write them in the shared cache and open its reader without any object store read.
 * <p>
 * What a reader reads at open is approximated with the rule of the recovery header warming
 * ({@code SharedBlobCacheWarmingService.ShardWarmer#addFile}): metadata files (see {@link LuceneFilesExtensions#isMetadata()}),
 * {@code segments_N}, generational files and live docs whole; for the other files their header (the first
 * {@link BlobCacheBufferedIndexInput#BUFFER_SIZE} bytes, which is what the first buffered read of a header fetches) and their
 * {@link CodecUtil#footerLength() footer}; for compound files, the same per entry of the {@code .cfe}. Files with replicated header/footer
 * ranges ({@link BlobFileRanges}) are read by the reader from the copies next to their compound commit header, so those copies are
 * carried too. Anything Lucene reads at open beyond this is not covered and is read from the object store as before.
 * <p>
 * The bytes are carried as blob ranges aligned to cache pages ({@link SharedBytes#PAGE_SIZE}), so that they can be written in the
 * shared cache as they are (cache writes are page aligned).
 */
public final class CommitPointerOpenBytes {

    private CommitPointerOpenBytes() {}

    /** Ranges of a blob closer than this are read from the object store with one GET on the index node. */
    static final long READ_COALESCE_GAP = 256 * 1024;

    /**
     * {@code bytes} of {@code blobFile} starting at {@code position} (aligned to a cache page; the end is aligned too, unless it is the
     * end of the blob).
     */
    public record OpenRange(BlobFile blobFile, long position, BytesReference bytes) implements Writeable {

        public OpenRange {
            Objects.requireNonNull(blobFile);
            Objects.requireNonNull(bytes);
            assert position % SharedBytes.PAGE_SIZE == 0 : position;
        }

        public OpenRange(StreamInput in) throws IOException {
            this(new BlobFile(in), in.readVLong(), in.readBytesReference());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            blobFile.writeTo(out);
            out.writeVLong(position);
            out.writeBytesReference(bytes);
        }

        public long end() {
            return position + bytes.length();
        }
    }

    /** Reads a range of a blob; may return fewer bytes than asked for at the end of the blob. */
    @FunctionalInterface
    public interface BlobRangeReader {
        BytesReference read(BlobFile blobFile, long position, int length) throws IOException;
    }

    /**
     * Whether opening a reader reads the whole file (same rule as the recovery header warming).
     */
    static boolean readsWholeFile(String fileName, long length) {
        final LuceneFilesExtensions extension = LuceneFilesExtensions.fromFile(fileName);
        return extension == null // segments_N
            || extension.isMetadata()
            || extension == LuceneFilesExtensions.LIV
            || StatelessCompoundCommit.isGenerationalFile(fileName)
            || length <= BlobCacheBufferedIndexInput.BUFFER_SIZE + CodecUtil.footerLength();
    }

    /**
     * The (unaligned) blob ranges that opening a reader on {@code commit} reads, per blob.
     *
     * @param fileRanges the ranges of the commit files as the search shard installs them (with replicated ranges if any)
     * @param cfeEntries the entries of a {@code .cfe} file of the commit, by file name
     */
    static Map<BlobFile, List<ByteRange>> neededRanges(
        StatelessCompoundCommit commit,
        Map<String, BlobFileRanges> fileRanges,
        Map<String, Map<String, Lucene90CompoundEntriesReader.FileEntry>> cfeEntries
    ) {
        final Map<BlobFile, List<ByteRange>> needed = new HashMap<>();
        for (var entry : commit.commitFiles().entrySet()) {
            final String fileName = entry.getKey();
            final BlobLocation location = entry.getValue();
            final List<ByteRange> ranges = needed.computeIfAbsent(location.blobFile(), k -> new ArrayList<>());
            if (LuceneFilesExtensions.fromFile(fileName) == LuceneFilesExtensions.CFS
                && readsWholeFile(fileName, location.fileLength()) == false) {
                final var entries = cfeEntries.get(fileName.substring(0, fileName.length() - "cfs".length()) + "cfe");
                addHeaderAndFooter(ranges, location.offset(), location.fileLength());
                if (entries != null) {
                    for (var cfsEntry : entries.entrySet()) {
                        addFile(ranges, cfsEntry.getKey(), location.offset() + cfsEntry.getValue().offset(), cfsEntry.getValue().length());
                    }
                }
            } else {
                addFile(ranges, fileName, location.offset(), location.fileLength());
            }
            final BlobFileRanges blobFileRanges = fileRanges.get(fileName);
            if (blobFileRanges != null && blobFileRanges.hasReplicatedRanges()) {
                // header/footer reads of this file are served from their copies next to the compound commit header
                blobFileRanges.forEachReplicatedRange((copy, length) -> ranges.add(ByteRange.of(copy, copy + length)));
            }
        }
        return needed;
    }

    private static void addFile(List<ByteRange> ranges, String fileName, long offset, long length) {
        if (length <= 0) {
            return;
        }
        if (readsWholeFile(fileName, length)) {
            ranges.add(ByteRange.of(offset, offset + length));
        } else {
            addHeaderAndFooter(ranges, offset, length);
        }
    }

    private static void addHeaderAndFooter(List<ByteRange> ranges, long offset, long length) {
        ranges.add(ByteRange.of(offset, offset + Math.min(length, BlobCacheBufferedIndexInput.BUFFER_SIZE)));
        final long footer = Math.max(0L, length - CodecUtil.footerLength());
        ranges.add(ByteRange.of(offset + footer, offset + length));
    }

    /**
     * Aligns the ranges to cache pages and merges those that overlap or touch.
     */
    static List<ByteRange> alignAndMerge(List<ByteRange> ranges) {
        final List<ByteRange> aligned = new ArrayList<>(ranges.size());
        for (ByteRange range : ranges) {
            aligned.add(
                ByteRange.of(
                    BlobCacheUtils.roundDownToAlignedSize(range.start(), SharedBytes.PAGE_SIZE),
                    BlobCacheUtils.roundUpToAlignedSize(range.end(), SharedBytes.PAGE_SIZE)
                )
            );
        }
        aligned.sort(Comparator.comparingLong(ByteRange::start));
        final List<ByteRange> merged = new ArrayList<>();
        for (ByteRange range : aligned) {
            if (merged.isEmpty() == false && merged.getLast().end() >= range.start()) {
                final ByteRange last = merged.removeLast();
                merged.add(ByteRange.of(last.start(), Math.max(last.end(), range.end())));
            } else {
                merged.add(range);
            }
        }
        return merged;
    }

    /** Statistics of one {@link #collect} call, for the write cost of the pointer. */
    public record Stats(long bytes, int ranges, int reads, long bytesRead) {}

    /**
     * Collects the open-time bytes of {@code commit}.
     *
     * @param fileRanges the commit's file ranges as the search shard installs them
     * @param reader     reads blob ranges from the object store
     * @param maxBytes   the bound on the carried bytes
     * @param cache      bytes of aligned ranges read for earlier commits, reused and updated (only the ranges of this commit are kept)
     * @param cfeCache   parsed {@code .cfe} files of earlier commits, reused and updated
     * @param stats      receives the statistics of the call (one element), may be {@code null}
     * @return the ranges, or {@code null} if they exceed {@code maxBytes}
     */
    @Nullable
    public static List<OpenRange> collect(
        StatelessCompoundCommit commit,
        Map<String, BlobFileRanges> fileRanges,
        BlobRangeReader reader,
        long maxBytes,
        Map<RangeKey, BytesReference> cache,
        Map<String, Map<String, Lucene90CompoundEntriesReader.FileEntry>> cfeCache,
        @Nullable Stats[] stats
    ) throws IOException {
        int reads = 0;
        long bytesRead = 0L;
        // the .cfe files are needed to locate the entries of the .cfs files (read whole, they are small)
        final Map<String, Map<String, Lucene90CompoundEntriesReader.FileEntry>> cfeEntries = new HashMap<>();
        for (var entry : commit.commitFiles().entrySet()) {
            if (LuceneFilesExtensions.fromFile(entry.getKey()) == LuceneFilesExtensions.CFE) {
                final String cfeKey = entry.getKey() + '@' + entry.getValue();
                var entries = cfeCache.get(cfeKey);
                if (entries == null) {
                    final BlobLocation location = entry.getValue();
                    final BytesReference bytes = reader.read(
                        location.blobFile(),
                        location.offset(),
                        Math.toIntExact(location.fileLength())
                    );
                    reads++;
                    bytesRead += bytes.length();
                    entries = Lucene90CompoundEntriesReader.readEntries(new ByteArrayDataInput(BytesReference.toBytes(bytes)));
                }
                cfeEntries.put(entry.getKey(), entries);
                cfeEntries.put(cfeKey, entries);
            }
        }
        cfeCache.keySet().retainAll(cfeEntries.keySet());
        for (var entry : cfeEntries.entrySet()) {
            if (entry.getKey().indexOf('@') >= 0) {
                cfeCache.put(entry.getKey(), entry.getValue());
            }
        }

        final Map<BlobFile, List<ByteRange>> needed = neededRanges(commit, fileRanges, cfeEntries);
        // bound on the merged aligned ranges before reading anything
        final Map<BlobFile, List<ByteRange>> aligned = new TreeMap<>(Comparator.comparing(BlobFile::blobName));
        long total = 0L;
        int count = 0;
        for (var entry : needed.entrySet()) {
            final List<ByteRange> merged = alignAndMerge(entry.getValue());
            aligned.put(entry.getKey(), merged);
            for (ByteRange range : merged) {
                total += range.length();
                count++;
            }
        }
        if (total > maxBytes) {
            if (stats != null) {
                stats[0] = new Stats(total, count, reads, bytesRead);
            }
            return null;
        }

        final List<OpenRange> result = new ArrayList<>(count);
        final Map<RangeKey, BytesReference> used = new HashMap<>();
        for (var entry : aligned.entrySet()) {
            final BlobFile blobFile = entry.getKey();
            final List<ByteRange> ranges = entry.getValue();
            int i = 0;
            while (i < ranges.size()) {
                final RangeKey key = new RangeKey(blobFile, ranges.get(i).start(), ranges.get(i).end());
                final BytesReference cached = cache.get(key);
                if (cached != null) {
                    used.put(key, cached);
                    result.add(new OpenRange(blobFile, key.start(), cached));
                    i++;
                    continue;
                }
                // one GET for this range and the following uncached ones that are close enough
                int j = i + 1;
                while (j < ranges.size()
                    && ranges.get(j).start() - ranges.get(j - 1).end() < READ_COALESCE_GAP
                    && cache.containsKey(new RangeKey(blobFile, ranges.get(j).start(), ranges.get(j).end())) == false) {
                    j++;
                }
                final long start = ranges.get(i).start();
                final long end = ranges.get(j - 1).end();
                final BytesReference bytes = reader.read(blobFile, start, Math.toIntExact(end - start));
                reads++;
                bytesRead += bytes.length();
                for (int k = i; k < j; k++) {
                    final ByteRange range = ranges.get(k);
                    final int from = Math.toIntExact(range.start() - start);
                    if (from >= bytes.length()) {
                        break; // past the end of the blob
                    }
                    // the last range may end at the end of the blob, before its aligned end
                    final BytesReference slice = bytes.slice(from, Math.toIntExact(Math.min(range.end() - start, bytes.length()) - from));
                    final RangeKey rangeKey = new RangeKey(blobFile, range.start(), range.end());
                    used.put(rangeKey, slice);
                    result.add(new OpenRange(blobFile, range.start(), slice));
                }
                i = j;
            }
        }
        cache.clear();
        cache.putAll(used);
        if (stats != null) {
            stats[0] = new Stats(total, count, reads, bytesRead);
        }
        return result;
    }

    /** An aligned range of a blob, the key of the bytes reused between commits. */
    public record RangeKey(BlobFile blobFile, long start, long end) {}
}
