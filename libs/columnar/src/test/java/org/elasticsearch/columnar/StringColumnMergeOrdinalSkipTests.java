/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LogDocMergePolicy;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.columnar.numeric.MultiLevelSkipIndexCodec;
import org.elasticsearch.columnar.numeric.NumericPipeline;
import org.elasticsearch.columnar.string.ColumnarStringBinaryDocValues;
import org.elasticsearch.columnar.string.DictionaryPolicy;
import org.elasticsearch.columnar.string.StringColumnOptions;
import org.elasticsearch.columnar.string.StringColumnOptionsSelector;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static org.elasticsearch.columnar.ColumnarTestUtils.columnarBinaryFieldType;
import static org.elasticsearch.columnar.ColumnarTestUtils.columnarCodec;
import static org.elasticsearch.columnar.ColumnarTestUtils.stringPayload;
import static org.hamcrest.Matchers.greaterThan;

public class StringColumnMergeOrdinalSkipTests extends ESTestCase {

    private static final String FIELD = "keyword";
    private static final String ID = "id";
    private static final int INTERVAL = MultiLevelSkipIndexCodec.INTERVAL_SIZE;
    private static final StringColumnOptions OPTIONS = StringColumnOptions.DEFAULT.withPolicies(
        new DictionaryPolicy(512 * 1024, 0.0, Double.MAX_VALUE),
        StringColumnOptions.DEFAULT_SUMMARY
    );

    public void testAMergedColumnBuildsItsIndexAndAnswersRangesAsAScanDoes() throws IOException {
        final int segments = between(2, 4);
        final int runs = between(1, 3);
        final String[] values = clustered(segments * (INTERVAL + between(0, INTERVAL)), runs, between(20, 60));
        final boolean[] deleted = new boolean[values.length];
        for (int doc = 0; doc < values.length; doc++) {
            deleted[doc] = randomInt(7) == 0;
        }

        final long withIndex;
        final long withoutIndex;
        try (Directory indexed = newDirectory(); Directory unindexed = newDirectory()) {
            writeSegments(indexed, OPTIONS, values, segments);
            withIndex = mergeAndCheck(indexed, OPTIONS, values, deleted);
            writeSegments(unindexed, OPTIONS.withoutSkipIndex(), values, segments);
            withoutIndex = mergeAndCheck(unindexed, OPTIONS.withoutSkipIndex(), values, deleted);
        }
        if (runs == 1) {
            assertEquals("a column read in order needs no index", withoutIndex, withIndex);
        } else {
            assertThat("the merged column carries the ordinal skip index", withIndex, greaterThan(withoutIndex));
        }
    }

    public void testAMergeIndexesSegmentsThatHadEscapes() throws IOException {
        final int segments = 2 * between(1, 2);
        final String[] values = clustered(segments * (INTERVAL + between(0, INTERVAL)), between(2, 3), between(20, 60));
        final int perSegment = values.length / segments;
        for (int segment = 0; segment < segments; segment++) {
            values[segment * perSegment + between(0, perSegment - 1)] = "pair-" + segment / 2;
        }
        final boolean[] deleted = new boolean[values.length];
        try (Directory indexed = newDirectory(); Directory unindexed = newDirectory()) {
            writeSegments(indexed, OPTIONS, values, segments);
            writeSegments(unindexed, OPTIONS.withoutSkipIndex(), values, segments);
            assertEquals("a flushed segment with escapes has no index", skipFileBytes(unindexed), skipFileBytes(indexed));
            final long withIndex = mergeAndCheck(indexed, OPTIONS, values, deleted);
            final long withoutIndex = mergeAndCheck(unindexed, OPTIONS.withoutSkipIndex(), values, deleted);
            assertThat("the merged column has no escapes and carries the index", withIndex, greaterThan(withoutIndex));
        }
    }

    private void writeSegments(Directory dir, StringColumnOptions options, String[] values, int segments) throws IOException {
        final int perSegment = values.length / segments;
        try (IndexWriter writer = new IndexWriter(dir, writerConfig(NoMergePolicy.INSTANCE, options))) {
            for (int doc = 0; doc < values.length; doc++) {
                final Document document = new Document();
                document.add(new StringField(ID, Integer.toString(doc), Field.Store.NO));
                document.add(new Field(FIELD, stringPayload(values[doc]), columnarBinaryFieldType()));
                writer.addDocument(document);
                if ((doc + 1) % perSegment == 0) {
                    writer.commit();
                }
            }
            writer.commit();
        }
    }

    private long mergeAndCheck(Directory dir, StringColumnOptions options, String[] values, boolean[] deleted) throws IOException {
        final LogDocMergePolicy mergePolicy = new LogDocMergePolicy();
        mergePolicy.setNoCFSRatio(0.0);
        try (IndexWriter writer = new IndexWriter(dir, writerConfig(mergePolicy, options))) {
            for (int doc = 0; doc < values.length; doc++) {
                if (deleted[doc]) {
                    writer.deleteDocuments(new Term(ID, Integer.toString(doc)));
                }
            }
            writer.forceMerge(1);
        }
        try (DirectoryReader reader = DirectoryReader.open(dir)) {
            assertEquals(1, reader.leaves().size());
            assertRangesMatchAScan(reader.leaves().get(0).reader().getBinaryDocValues(FIELD), values, deleted);
        }
        return skipFileBytes(dir);
    }

    private static long skipFileBytes(Directory dir) throws IOException {
        long skipBytes = 0;
        for (String file : dir.listAll()) {
            if ("cns".equals(IndexFileNames.getExtension(file))) {
                skipBytes += dir.fileLength(file);
            }
        }
        return skipBytes;
    }

    private static void assertRangesMatchAScan(Object docValues, String[] values, boolean[] deleted) throws IOException {
        assertTrue(docValues instanceof ColumnarStringBinaryDocValues);
        final List<BytesRef> kept = new ArrayList<>();
        for (int doc = 0; doc < values.length; doc++) {
            if (deleted[doc] == false) {
                kept.add(new BytesRef(values[doc]));
            }
        }
        for (int query = 0; query < 20; query++) {
            final BytesRef lower = rarely() ? null : kept.get(between(0, kept.size() - 1));
            final BytesRef upper = rarely() ? null : kept.get(between(0, kept.size() - 1));
            final boolean includeLower = randomBoolean();
            final boolean includeUpper = randomBoolean();
            final List<Integer> expected = new ArrayList<>();
            for (int doc = 0; doc < kept.size(); doc++) {
                if (inRange(kept.get(doc), lower, includeLower, upper, includeUpper)) {
                    expected.add(doc);
                }
            }
            final DocIdSetIterator matches = ((ColumnarStringBinaryDocValues) docValues).reader()
                .matchRange(lower, includeLower, upper, includeUpper);
            final List<Integer> actual = new ArrayList<>();
            for (int doc = matches.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = matches.nextDoc()) {
                actual.add(doc);
            }
            assertEquals("range " + lower + " " + includeLower + " " + upper + " " + includeUpper, expected, actual);
        }
    }

    private static IndexWriterConfig writerConfig(MergePolicy mergePolicy, StringColumnOptions options) {
        final ColumNARDocValuesFormat format = new ColumNARDocValuesFormat(
            (field, type) -> NumericPipeline::defaultPipeline,
            field -> ColumnarFieldType.STRING,
            ColumNARDocValuesFormat.DEFAULT_BLOCK_SIZE,
            StringColumnOptionsSelector.always(options)
        );
        return new IndexWriterConfig().setCodec(columnarCodec(format)).setUseCompoundFile(false).setMergePolicy(mergePolicy);
    }

    private static String[] clustered(int count, int runs, int terms) {
        final String[] values = new String[count];
        final int perRun = (count + runs - 1) / runs;
        for (int doc = 0; doc < count; doc++) {
            values[doc] = String.format(Locale.ROOT, "term-%05d", (doc % perRun) * terms / perRun);
        }
        return values;
    }

    private static boolean inRange(BytesRef value, BytesRef lower, boolean includeLower, BytesRef upper, boolean includeUpper) {
        if (lower != null) {
            final int cmp = value.compareTo(lower);
            if (cmp < 0 || (cmp == 0 && includeLower == false)) {
                return false;
            }
        }
        if (upper != null) {
            final int cmp = value.compareTo(upper);
            return cmp < 0 || (cmp == 0 && includeUpper);
        }
        return true;
    }
}
