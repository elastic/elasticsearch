/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.LogDocMergePolicy;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.columnar.ColumNARDocValuesFormat;
import org.elasticsearch.columnar.ColumnarFieldType;
import org.elasticsearch.columnar.FormatVersion;
import org.elasticsearch.columnar.numeric.MultiLevelSkipIndexCodec;
import org.elasticsearch.columnar.numeric.NumericPipeline;
import org.elasticsearch.columnar.substrate.ChunkBounds;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.columnar.ColumnarTestUtils.columnarBinaryFieldType;
import static org.elasticsearch.columnar.ColumnarTestUtils.columnarCodec;
import static org.hamcrest.Matchers.greaterThan;

public class PlainStringColumnLifecycleTests extends ESTestCase {

    private static final String FIELD = "keyword";
    private static final String ID = "id";
    private static final int INTERVAL = MultiLevelSkipIndexCodec.INTERVAL_SIZE;
    private static final DictionaryPolicy FORCED = new DictionaryPolicy(512 * 1024, 0.0, Double.MAX_VALUE);

    public void testResultsSurviveFlushReopenMergeAndForceMergeAcrossLayoutsAndBounds() throws IOException {
        final int segments = between(3, 5);
        final BytesRef[] pool = PlainSkipTestValues.sortedPool(between(8, 40));
        final BytesRef[][] docSlots = descendingColumn(segments * (INTERVAL + between(1, INTERVAL / 2)), pool);
        final List<StringColumnOptions> perSegment = List.of(plain(true), plain(false), dictionary(), plain(true), dictionary());
        final boolean[] deleted = new boolean[docSlots.length];
        for (int doc = 0; doc < deleted.length; doc++) {
            deleted[doc] = randomInt(9) == 0;
        }
        for (StringColumnOptions mergeOptions : List.of(plain(true), plain(false), dictionary())) {
            try (Directory dir = newDirectory()) {
                final int[] starts = writeSegments(dir, docSlots, segments, perSegment, FormatVersion.CURRENT);
                try (DirectoryReader reader = DirectoryReader.open(dir)) {
                    assertEquals(segments, reader.leaves().size());
                    checkLeaves(reader, docSlots, starts, pool);
                }
                try (DirectoryReader reopened = DirectoryReader.open(dir)) {
                    checkLeaves(reopened, docSlots, starts, pool);
                }
                final BytesRef[][] kept = merge(dir, mergeOptions, docSlots, deleted, FormatVersion.CURRENT);
                try (DirectoryReader merged = DirectoryReader.open(dir)) {
                    assertEquals(1, merged.leaves().size());
                    checkLeaves(merged, kept, new int[] { 0 }, pool);
                }
            }
        }
    }

    public void testMergedBoundsComeFromTheValuesTheMergeWrites() throws IOException {
        final int count = 6 * INTERVAL;
        final BytesRef outlier = new BytesRef("zzzzzzzz");
        final BytesRef[][] docSlots = new BytesRef[count][];
        for (int doc = 0; doc < count; doc++) {
            final int band = 5 - doc / INTERVAL;
            docSlots[doc] = new BytesRef[] {
                new BytesRef("band-" + band + "-" + String.format(java.util.Locale.ROOT, "%08d", between(0, 99_999_999))) };
        }
        docSlots[10] = new BytesRef[] { outlier };
        final Counter seeks = new Counter();
        try (Directory dir = new SeekCountingDirectory(newDirectory(), seeks)) {
            writeSegments(dir, docSlots, 2, List.of(plain(true)), FormatVersion.CURRENT);
            long before = 0;
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                for (LeafReaderContext leaf : reader.leaves()) {
                    final StringColumnReader column = columnOf(leaf);
                    final long start = seeks.get();
                    final List<Integer> matches = StringMatchOracle.matched(column.matchTerm(outlier));
                    before += seeks.get() - start;
                    assertEquals(leaf.docBase == 0 ? List.of(10) : List.of(), matches);
                }
            }
            assertThat("the outlier forces its interval to be read", before, greaterThan(0L));
            final boolean[] deleted = new boolean[count];
            deleted[10] = true;
            merge(dir, plain(true), docSlots, deleted, FormatVersion.CURRENT);
            try (DirectoryReader merged = DirectoryReader.open(dir)) {
                final long start = seeks.get();
                assertEquals(List.of(), StringMatchOracle.matched(columnOf(merged.leaves().get(0)).matchTerm(outlier)));
                assertEquals("no interval holds the deleted outlier any more", 0L, seeks.get() - start);
            }
        }
    }

    public void testTheMergedSegmentFollowsThePolicyOfTheMergeNotTheLayoutOfItsInputs() throws IOException {
        final BytesRef[] pool = PlainSkipTestValues.sortedPool(12);
        final BytesRef[][] docSlots = descendingColumn(3 * INTERVAL, pool);
        final boolean[] deleted = new boolean[docSlots.length];
        final long[] bytes = new long[4];
        final List<StringColumnOptions> merges = List.of(plain(true), plain(false), dictionary(), dictionary());
        final List<StringColumnOptions> inputs = List.of(plain(true), plain(true), plain(true), plain(false));
        for (int i = 0; i < merges.size(); i++) {
            try (Directory dir = newDirectory()) {
                writeSegments(dir, docSlots, 3, List.of(inputs.get(i)), FormatVersion.CURRENT);
                merge(dir, merges.get(i), docSlots, deleted, FormatVersion.CURRENT);
                bytes[i] = skipFileBytes(dir);
            }
        }
        assertThat("a plain merge with bounds writes them", bytes[0], greaterThan(bytes[1]));
        assertThat("a merge into a dictionary writes the ordinal index, not plain bounds", bytes[2], greaterThan(0L));
        assertEquals("inputs with and without bounds merge into the same dictionary", bytes[2], bytes[3]);
    }

    public void testABaselineSegmentWritesNoBoundsAndMergesIntoTheCurrentFormat() throws IOException {
        final int segments = between(2, 4);
        final BytesRef[] pool = PlainSkipTestValues.sortedPool(between(8, 40));
        final BytesRef[][] docSlots = descendingColumn(segments * (INTERVAL + between(1, INTERVAL / 2)), pool);
        final boolean[] deleted = new boolean[docSlots.length];
        for (int doc = 0; doc < deleted.length; doc++) {
            deleted[doc] = randomInt(9) == 0;
        }
        try (Directory baseline = newDirectory(); Directory withoutBounds = newDirectory(); Directory current = newDirectory()) {
            final int[] starts = writeSegments(baseline, docSlots, segments, List.of(plain(true)), FormatVersion.BASELINE);
            writeSegments(withoutBounds, docSlots, segments, List.of(plain(false)), FormatVersion.BASELINE);
            writeSegments(current, docSlots, segments, List.of(plain(true)), FormatVersion.CURRENT);

            assertHeaderVersions(baseline, FormatVersion.BASELINE);
            assertHeaderVersions(current, FormatVersion.CURRENT);
            assertEquals(
                "a baseline segment has no room for the bounds it was asked for",
                skipFileBytes(withoutBounds),
                skipFileBytes(baseline)
            );
            assertThat("the current format writes them", skipFileBytes(current), greaterThan(skipFileBytes(baseline)));

            try (DirectoryReader reader = DirectoryReader.open(baseline)) {
                assertEquals(segments, reader.leaves().size());
                checkLeaves(reader, docSlots, starts, pool);
            }
            final BytesRef[][] kept = merge(baseline, plain(true), docSlots, deleted, FormatVersion.CURRENT);
            assertHeaderVersions(baseline, FormatVersion.CURRENT);
            assertThat("the merge writes the bounds the baseline segments lacked", skipFileBytes(baseline), greaterThan(0L));
            try (DirectoryReader merged = DirectoryReader.open(baseline)) {
                assertEquals(1, merged.leaves().size());
                checkLeaves(merged, kept, new int[] { 0 }, pool);
            }
        }
    }

    private static void checkLeaves(DirectoryReader reader, BytesRef[][] docSlots, int[] starts, BytesRef[] pool) throws IOException {
        for (int i = 0; i < reader.leaves().size(); i++) {
            final LeafReaderContext leaf = reader.leaves().get(i);
            final BytesRef[][] slice = Arrays.copyOfRange(docSlots, starts[i], starts[i] + leaf.reader().maxDoc());
            StringMatchOracle.checkRandomQueries(columnOf(leaf), slice, pool, 20);
        }
    }

    private static StringColumnReader columnOf(LeafReaderContext leaf) throws IOException {
        return ((ColumnarStringBinaryDocValues) leaf.reader().getBinaryDocValues(FIELD)).reader();
    }

    private static int[] writeSegments(
        Directory dir,
        BytesRef[][] docSlots,
        int segments,
        List<StringColumnOptions> options,
        FormatVersion formatVersion
    ) throws IOException {
        final int[] starts = new int[segments];
        final int size = docSlots.length / segments;
        for (int segment = 0; segment < segments; segment++) {
            starts[segment] = segment * size;
            final int end = segment == segments - 1 ? docSlots.length : (segment + 1) * size;
            try (
                IndexWriter writer = new IndexWriter(
                    dir,
                    config(NoMergePolicy.INSTANCE, options.get(segment % options.size()), formatVersion)
                )
            ) {
                for (int doc = starts[segment]; doc < end; doc++) {
                    writer.addDocument(document(doc, docSlots[doc]));
                }
            }
        }
        return starts;
    }

    private static BytesRef[][] merge(
        Directory dir,
        StringColumnOptions options,
        BytesRef[][] docSlots,
        boolean[] deleted,
        FormatVersion formatVersion
    ) throws IOException {
        final LogDocMergePolicy policy = new LogDocMergePolicy();
        policy.setNoCFSRatio(0.0);
        try (IndexWriter writer = new IndexWriter(dir, config(policy, options, formatVersion))) {
            final List<BytesRef[]> kept = new ArrayList<>();
            for (int doc = 0; doc < docSlots.length; doc++) {
                if (deleted[doc]) {
                    writer.deleteDocuments(new Term(ID, Integer.toString(doc)));
                } else {
                    kept.add(docSlots[doc]);
                }
            }
            writer.forceMerge(1);
            return kept.toArray(new BytesRef[0][]);
        }
    }

    private static Document document(int doc, BytesRef[] slots) {
        final Document document = new Document();
        document.add(new StringField(ID, Integer.toString(doc), Field.Store.NO));
        if (slots != null) {
            document.add(
                new Field(
                    FIELD,
                    BytesRef.deepCopyOf(new StringBinaryPayload.Builder().encode(Arrays.asList(slots))),
                    columnarBinaryFieldType()
                )
            );
        }
        return document;
    }

    private static void assertHeaderVersions(Directory dir, FormatVersion expected) throws IOException {
        int checked = 0;
        for (String file : dir.listAll()) {
            final String extension = IndexFileNames.getExtension(file);
            if (extension != null && Set.of("cnd", "cna", "cnl", "cnn", "cnm", "cns").contains(extension)) {
                try (IndexInput in = dir.openInput(file, IOContext.READONCE)) {
                    CodecUtil.readBEInt(in);
                    in.readString();
                    assertEquals(file, expected.version(), CodecUtil.readBEInt(in));
                    checked++;
                }
            }
        }
        assertThat("the files of every segment were checked", checked, greaterThan(0));
    }

    private static long skipFileBytes(Directory dir) throws IOException {
        long bytes = 0;
        for (String file : dir.listAll()) {
            if ("cns".equals(IndexFileNames.getExtension(file))) {
                bytes += dir.fileLength(file);
            }
        }
        return bytes;
    }

    private static BytesRef[][] descendingColumn(int count, BytesRef[] pool) {
        final BytesRef[][] docSlots = new BytesRef[count][];
        for (int doc = 0; doc < count; doc++) {
            if (randomInt(9) == 0) {
                continue;
            }
            final int index = Math.max(
                0,
                Math.min(pool.length - 1, pool.length - 1 - (int) ((long) doc * pool.length / count) + between(-1, 1))
            );
            docSlots[doc] = randomInt(7) == 0 ? new BytesRef[] { pool[index], randomFrom(pool) } : new BytesRef[] { pool[index] };
        }
        return docSlots;
    }

    private static IndexWriterConfig config(MergePolicy mergePolicy, StringColumnOptions options, FormatVersion formatVersion) {
        final ColumNARDocValuesFormat format = new ColumNARDocValuesFormat(
            (field, type) -> NumericPipeline::defaultPipeline,
            field -> ColumnarFieldType.STRING,
            ColumNARDocValuesFormat.DEFAULT_BLOCK_SIZE,
            StringColumnOptionsSelector.always(options),
            formatVersion
        );
        return new IndexWriterConfig().setCodec(columnarCodec(format)).setUseCompoundFile(false).setMergePolicy(mergePolicy);
    }

    private static StringColumnOptions plain(boolean bounded) {
        return StringColumnOptions.DEFAULT.withPolicies(DictionaryPolicy.NONE, SummaryPolicy.NONE)
            .withSizes(smallChunks())
            .withPlainSkipIndex(bounded);
    }

    private static StringColumnOptions dictionary() {
        return StringColumnOptions.DEFAULT.withPolicies(FORCED, StringColumnOptions.DEFAULT_SUMMARY)
            .withSizes(smallChunks())
            .withPlainSkipIndex(true);
    }

    private static StringColumnOptions.Sizes smallChunks() {
        final StringColumnOptions.Sizes defaults = StringColumnOptions.DEFAULT_SIZES;
        return new StringColumnOptions.Sizes(
            defaults.valuesPerBlock(),
            new ChunkBounds(4096, StringColumnOptions.DEFAULT_MAX_VALUES_PER_CHUNK),
            defaults.escapeChunks(),
            defaults.packedOrdinalBlockSize(),
            defaults.compressedOrdinalBlockSize(),
            defaults.escapeRankBlockSize(),
            defaults.slotCountsBlockSize(),
            defaults.lengthBlockSize()
        );
    }
}
