/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.commits;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.util.StringHelper;
import org.elasticsearch.blobcache.common.ByteRange;
import org.elasticsearch.blobcache.shared.SharedBytes;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.stateless.cache.Lucene90CompoundEntriesReader;
import org.elasticsearch.xpack.stateless.engine.PrimaryTermAndGeneration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class CommitPointerOpenBytesTests extends ESTestCase {

    private static final int PAGE = SharedBytes.PAGE_SIZE;

    /**
     * A BCC blob with a {@code segments_N}, a segment info, a compound segment ({@code .cfe} + {@code .cfs} with a metadata entry and a
     * doc values entry), a large non-compound data file and a generational live docs file. Files are page aligned and far enough apart
     * that the pages of one never cover another's bytes, so that coverage can be checked byte by byte.
     */
    private record Layout(byte[] blob, BlobFile blobFile, StatelessCompoundCommit commit, Map<String, long[]> neededFileRanges) {}

    private static Layout layout() throws IOException {
        final var termAndGen = new PrimaryTermAndGeneration(1, 7);
        final BlobFile blobFile = new BlobFile(BatchedCompoundCommit.PREFIX + 7, termAndGen);
        final Map<String, BlobLocation> files = new HashMap<>();
        final Map<String, long[]> needed = new HashMap<>(); // name -> {start, end} pairs (blob positions)
        final int fnmLength = randomIntBetween(100, 3000);
        final int dvdLength = randomIntBetween(40_000, 60_000);
        final long cfsOffset = 4 * PAGE;
        final long fnmOffset = 64; // within the .cfs
        final long dvdOffset = 2 * PAGE; // within the .cfs
        final long cfsLength = dvdOffset + dvdLength + 16;

        // a real .cfe naming the two entries
        final ByteBuffersDataOutput cfe = new ByteBuffersDataOutput();
        CodecUtil.writeIndexHeader(cfe, "Lucene90CompoundEntries", 0, StringHelper.randomId(), "");
        cfe.writeVInt(2);
        cfe.writeString(".fnm");
        cfe.writeLong(fnmOffset);
        cfe.writeLong(fnmLength);
        cfe.writeString("_Lucene90_0.dvd");
        cfe.writeLong(dvdOffset);
        cfe.writeLong(dvdLength);
        final byte[] cfeBytes = cfe.toArrayCopy();

        final long timOffset = 32 * PAGE;
        final int timLength = randomIntBetween(100_000, 150_000);
        final long livOffset = timOffset + 40 * PAGE;
        final int blobLength = Math.toIntExact(livOffset + randomIntBetween(10, 100)); // the blob ends inside the last page

        files.put("segments_5", new BlobLocation(blobFile, 0, 300));
        files.put("_0.si", new BlobLocation(blobFile, 300, 400));
        files.put("_0.cfe", new BlobLocation(blobFile, 2 * PAGE, cfeBytes.length));
        files.put("_0.cfs", new BlobLocation(blobFile, cfsOffset, cfsLength));
        files.put("_1.tim", new BlobLocation(blobFile, timOffset, timLength));
        files.put("_1_2.liv", new BlobLocation(blobFile, livOffset, blobLength - livOffset));

        needed.put("segments_5", new long[] { 0, 300 });
        needed.put("_0.si", new long[] { 300, 700 });
        needed.put("_0.cfe", new long[] { 2 * PAGE, 2 * PAGE + cfeBytes.length });
        needed.put("_0.cfs header", new long[] { cfsOffset, cfsOffset + 1024 });
        needed.put("_0.cfs footer", new long[] { cfsOffset + cfsLength - 16, cfsOffset + cfsLength });
        needed.put(".fnm", new long[] { cfsOffset + fnmOffset, cfsOffset + fnmOffset + fnmLength });
        needed.put(".dvd header", new long[] { cfsOffset + dvdOffset, cfsOffset + dvdOffset + 1024 });
        needed.put(".dvd footer", new long[] { cfsOffset + dvdOffset + dvdLength - 16, cfsOffset + dvdOffset + dvdLength });
        needed.put("_1.tim header", new long[] { timOffset, timOffset + 1024 });
        needed.put("_1.tim footer", new long[] { timOffset + timLength - 16, timOffset + timLength });
        needed.put("_1_2.liv", new long[] { livOffset, blobLength });

        final byte[] blob = randomByteArrayOfLength(blobLength);
        System.arraycopy(cfeBytes, 0, blob, 2 * PAGE, cfeBytes.length);
        final var commit = new StatelessCompoundCommit(
            new ShardId(new Index("index", "uuid"), 0),
            termAndGen,
            1L,
            "_na_",
            files,
            blobLength,
            files.keySet(),
            0L,
            InternalFilesReplicatedRanges.EMPTY,
            Map.of(),
            null
        );
        return new Layout(blob, blobFile, commit, needed);
    }

    private static CommitPointerOpenBytes.BlobRangeReader reader(Layout layout, AtomicInteger reads) {
        return (blobFile, position, length) -> {
            assertThat(blobFile, equalTo(layout.blobFile()));
            reads.incrementAndGet();
            final int end = Math.toIntExact(Math.min(position + length, layout.blob().length));
            return new BytesArray(layout.blob(), Math.toIntExact(position), end - Math.toIntExact(position));
        };
    }

    private static Map<String, BlobFileRanges> plainRanges(StatelessCompoundCommit commit) {
        final Map<String, BlobFileRanges> ranges = new HashMap<>();
        commit.commitFiles().forEach((name, location) -> ranges.put(name, new BlobFileRanges(location)));
        return ranges;
    }

    public void testReadsWholeFile() {
        assertTrue(CommitPointerOpenBytes.readsWholeFile("segments_12", 100_000));
        assertTrue(CommitPointerOpenBytes.readsWholeFile("_0.si", 100_000));
        assertTrue(CommitPointerOpenBytes.readsWholeFile("_0.fnm", 100_000));
        assertTrue(CommitPointerOpenBytes.readsWholeFile("_0_Lucene90_0.dvm", 100_000));
        assertTrue(CommitPointerOpenBytes.readsWholeFile("_0.cfe", 100_000));
        assertTrue(CommitPointerOpenBytes.readsWholeFile("_0_3.liv", 100_000));
        assertTrue("generational doc values update", CommitPointerOpenBytes.readsWholeFile("_0_3_Lucene90_0.dvd", 100_000));
        assertTrue("small data file", CommitPointerOpenBytes.readsWholeFile("_0.tim", 1040));
        assertFalse(CommitPointerOpenBytes.readsWholeFile("_0.tim", 1041));
        assertFalse(CommitPointerOpenBytes.readsWholeFile("_0_Lucene90_0.dvd", 100_000));
        assertFalse(CommitPointerOpenBytes.readsWholeFile("_0.cfs", 100_000));
    }

    public void testAlignAndMerge() {
        final List<ByteRange> merged = CommitPointerOpenBytes.alignAndMerge(
            List.of(
                ByteRange.of(5 * PAGE + 10, 5 * PAGE + 20),
                ByteRange.of(10, 20),
                ByteRange.of(PAGE - 1, PAGE + 1),
                ByteRange.of(2 * PAGE, 2 * PAGE + 5),
                ByteRange.of(5 * PAGE + 100, 6 * PAGE + 1)
            )
        );
        assertThat(merged, equalTo(List.of(ByteRange.of(0, 3 * PAGE), ByteRange.of(5 * PAGE, 7 * PAGE))));
    }

    public void testCollectCoversOpenTimeBytesOnly() throws IOException {
        final Layout layout = layout();
        final AtomicInteger reads = new AtomicInteger();
        final Map<CommitPointerOpenBytes.RangeKey, BytesReference> cache = new HashMap<>();
        final Map<String, Map<String, Lucene90CompoundEntriesReader.FileEntry>> cfeCache = new HashMap<>();
        final CommitPointerOpenBytes.Stats[] stats = new CommitPointerOpenBytes.Stats[1];
        final var ranges = CommitPointerOpenBytes.collect(
            layout.commit(),
            plainRanges(layout.commit()),
            reader(layout, reads),
            Long.MAX_VALUE,
            cache,
            cfeCache,
            stats
        );
        assertThat(ranges, notNullValue());
        long total = 0;
        for (var range : ranges) {
            assertThat(range.position() % PAGE, equalTo(0L));
            assertTrue("aligned end or end of the blob: " + range, range.end() % PAGE == 0 || range.end() == layout.blob().length);
            final byte[] expected = new byte[range.bytes().length()];
            System.arraycopy(layout.blob(), Math.toIntExact(range.position()), expected, 0, expected.length);
            assertArrayEquals(expected, BytesReference.toBytes(range.bytes()));
            total += range.bytes().length();
        }
        assertThat(stats[0].ranges(), equalTo(ranges.size()));
        // the bound applies to the aligned ranges before reading (the last one may end earlier, at the end of the blob)
        final long alignedTotal = stats[0].bytes();
        assertThat(alignedTotal, greaterThanOrEqualTo(total));
        assertThat(alignedTotal - total, lessThan((long) PAGE));
        // every byte a reader reads at open is covered
        for (var entry : layout.neededFileRanges().entrySet()) {
            for (long pos = entry.getValue()[0]; pos < entry.getValue()[1]; pos++) {
                assertTrue(entry.getKey() + " byte " + pos + " not covered", covered(ranges, pos));
            }
        }
        // the bodies of data files are not carried
        final long timBody = layout.commit().commitFiles().get("_1.tim").offset() + 16 * PAGE;
        assertFalse(covered(ranges, timBody));
        final long dvdBody = layout.commit().commitFiles().get("_0.cfs").offset() + 2 * PAGE + 8 * PAGE;
        assertFalse(covered(ranges, dvdBody));
        assertThat(total, lessThanOrEqualTo(16L * PAGE));
        // ranges closer than the coalescing gap are read together: the .cfe (1 read) and then a few GETs, not one per range
        assertThat(reads.get(), lessThanOrEqualTo(3));

        // the next commit references the same files: nothing is read again
        reads.set(0);
        final var again = CommitPointerOpenBytes.collect(
            layout.commit(),
            plainRanges(layout.commit()),
            reader(layout, reads),
            Long.MAX_VALUE,
            cache,
            cfeCache,
            stats
        );
        assertThat(reads.get(), equalTo(0));
        assertThat(again.size(), equalTo(ranges.size()));

        // above the bound: nothing is carried and nothing is read
        reads.set(0);
        assertThat(
            CommitPointerOpenBytes.collect(
                layout.commit(),
                plainRanges(layout.commit()),
                reader(layout, reads),
                alignedTotal - 1,
                new HashMap<>(),
                cfeCache,
                stats
            ),
            nullValue()
        );
        assertThat(reads.get(), equalTo(0));
        assertThat(stats[0].bytes(), equalTo(alignedTotal));
    }

    public void testReplicatedCopiesAreCarried() throws IOException {
        // a commit whose files have header/footer copies next to the compound commit header: the reader reads the copies
        final var termAndGen = new PrimaryTermAndGeneration(1, 3);
        final var cc = org.elasticsearch.xpack.stateless.TestUtils.getCommitWithInternalFilesReplicatedRanges(
            new ShardId(new Index("index", "uuid"), 0),
            new BlobFile(BatchedCompoundCommit.PREFIX + 3, termAndGen),
            "_na_",
            0,
            4096
        );
        final var bcc = new BatchedCompoundCommit(termAndGen, List.of(cc));
        final var pointer = CommitPointer.create(1, "node", bcc, CommitPointer.computeReplicatedFileRanges(bcc));
        final var needed = CommitPointerOpenBytes.neededRanges(pointer.compoundCommit(), pointer.installedFileRanges(), Map.of());
        assertThat(pointer.replicatedFileRanges().size(), greaterThan(0));
        final List<ByteRange> all = new ArrayList<>();
        needed.values().forEach(all::addAll);
        for (var ranges : pointer.replicatedFileRanges().values()) {
            ranges.forEachReplicatedRange((copy, length) -> assertTrue(all.contains(ByteRange.of(copy, copy + length))));
        }
        assertThat(needed.keySet(), equalTo(Set.of(cc.commitFiles().values().iterator().next().blobFile())));
    }

    private static boolean covered(List<CommitPointerOpenBytes.OpenRange> ranges, long position) {
        for (var range : ranges) {
            if (range.position() <= position && position < range.end()) {
                return true;
            }
        }
        return false;
    }
}
