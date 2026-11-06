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
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LogDocMergePolicy;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.codec.profiler.ProfileCollector;
import org.elasticsearch.codec.profiler.Scopes;
import org.elasticsearch.codec.profiler.Scopes.Binding;
import org.elasticsearch.codec.profiler.TestCollectors;
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
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.columnar.ColumnarTestUtils.columnarBinaryFieldType;
import static org.elasticsearch.columnar.ColumnarTestUtils.columnarCodec;
import static org.elasticsearch.columnar.ColumnarTestUtils.stringPayload;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;

public class OrdinalSkipProfilingTests extends ESTestCase {

    private static final String FIELD = "keyword";
    private static final int INTERVAL = MultiLevelSkipIndexCodec.INTERVAL_SIZE;
    private static final int TERMS = 40;
    private static final StringColumnOptions INDEXED = StringColumnOptions.DEFAULT.withPolicies(
        new DictionaryPolicy(512 * 1024, 0.0, Double.MAX_VALUE),
        StringColumnOptions.DEFAULT_SUMMARY
    );

    public void testARangeOverAClusteredColumnReportsTheSpansItSkipped() throws IOException {
        final String[] values = clustered(12 * INTERVAL + between(0, INTERVAL), 4);
        try (Directory dir = mergedColumn(INDEXED, values); DirectoryReader reader = DirectoryReader.open(dir)) {
            final ProfileCollector stats = TestCollectors.elapsed();
            final List<Integer> matches = matchRange(open(stats, reader.leaves().get(0).reader()), term(0), term(1));

            final Map<String, Long> counted = TestCollectors.toKeyedValues(stats.snapshot());
            assertThat(
                "a run that holds only other terms is stepped over",
                counted.get("ColumNAR.ordinal_skip.intervals_rejected"),
                greaterThan(0L)
            );
            assertThat(counted.get("ColumNAR.ordinal_skip.docid_span"), greaterThan(0L));
            assertThat(counted.get("ColumNAR.ordinal_skip.docid_span"), lessThanOrEqualTo((long) values.length));
            assertEquals(expected(values, term(0), term(1)), matches);
        }
    }

    public void testProfilingDoesNotChangeTheMatches() throws IOException {
        final String[] values = clustered(3 * INTERVAL + between(0, INTERVAL), between(2, 4));
        try (Directory dir = mergedColumn(INDEXED, values); DirectoryReader reader = DirectoryReader.open(dir)) {
            final LeafReader leaf = reader.leaves().get(0).reader();
            for (int query = 0; query < 10; query++) {
                final int low = between(0, TERMS - 1);
                final BytesRef lower = term(low);
                final BytesRef upper = term(between(low, TERMS - 1));
                final List<Integer> profiled = matchRange(open(TestCollectors.elapsed(), leaf), lower, upper);
                assertEquals(matchRange((ColumnarStringBinaryDocValues) leaf.getBinaryDocValues(FIELD), lower, upper), profiled);
                assertEquals(expected(values, lower, upper), profiled);
            }
        }
    }

    public void testARangeIsTimedWhenTheCollectorMeasuresTimeAndNotOtherwise() throws IOException {
        final String[] values = clustered(12 * INTERVAL + between(0, INTERVAL), 4);
        try (Directory dir = mergedColumn(INDEXED, values); DirectoryReader reader = DirectoryReader.open(dir)) {
            final long[] now = { 0 };
            final ProfileCollector timed = TestCollectors.withClock(() -> now[0] += 25);
            final ProfileCollector countersOnly = TestCollectors.countersOnly();
            final List<Integer> timedMatches = matchRange(open(timed, reader.leaves().get(0).reader()), term(0), term(1));
            final List<Integer> plainMatches = matchRange(open(countersOnly, reader.leaves().get(0).reader()), term(0), term(1));

            assertEquals("timing does not change what matches", plainMatches, timedMatches);
            assertThat(TestCollectors.toKeyedValues(timed.snapshot()).get("ColumNAR.ordinal_skip.navigation_time_nanos"), greaterThan(0L));
            assertThat(TestCollectors.toKeyedValues(countersOnly.snapshot()), not(hasKey("ColumNAR.ordinal_skip.navigation_time_nanos")));
            assertEquals(
                "the same spans are skipped",
                TestCollectors.toKeyedValues(countersOnly.snapshot()).get("ColumNAR.ordinal_skip.docid_span"),
                TestCollectors.toKeyedValues(timed.snapshot()).get("ColumNAR.ordinal_skip.docid_span")
            );
        }
    }

    public void testAColumnWithoutASkipIndexReportsNoSkipMetricsAtAll() throws IOException {
        final String[] values = clustered(3 * INTERVAL, 3);
        try (Directory dir = mergedColumn(INDEXED.withoutSkipIndex(), values); DirectoryReader reader = DirectoryReader.open(dir)) {
            final ProfileCollector stats = TestCollectors.elapsed();
            matchRange(open(stats, reader.leaves().get(0).reader()), term(0), term(1));
            assertThat(TestCollectors.toKeyedValues(stats.snapshot()), not(hasKey("ColumNAR.ordinal_skip.intervals_rejected")));
            assertThat(TestCollectors.toKeyedValues(stats.snapshot()), not(hasKey("ColumNAR.ordinal_skip.docid_span")));
        }
    }

    public void testAColumnThatIsOnlyScannedReportsItsSkipMetricsAtZero() throws IOException {
        final String[] values = clustered(3 * INTERVAL, 3);
        try (Directory dir = mergedColumn(INDEXED, values); DirectoryReader reader = DirectoryReader.open(dir)) {
            final ProfileCollector stats = TestCollectors.elapsed();
            final ColumnarStringBinaryDocValues column = open(stats, reader.leaves().get(0).reader());
            while (column.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                column.binaryValue();
            }
            // NOTE: The metrics register when the reader is built, so a query that could have skipped and did not
            // shows zeros rather than nothing. Absent means the column has no skip index; see the test above.
            assertThat(TestCollectors.toKeyedValues(stats.snapshot()).get("ColumNAR.ordinal_skip.intervals_rejected"), equalTo(0L));
            assertThat(TestCollectors.toKeyedValues(stats.snapshot()).get("ColumNAR.ordinal_skip.docid_span"), equalTo(0L));
            assertThat(TestCollectors.toKeyedValues(stats.snapshot()).get("ColumNAR.ordinal_skip.navigation_time_nanos"), equalTo(0L));
        }
    }

    public void testASkipIndexedDictionaryScanReportsExactlyTheDictionaryLayoutMetricKeys() throws IOException {
        final String[] values = clustered(3 * INTERVAL, 3);
        try (Directory dir = mergedColumn(INDEXED, values); DirectoryReader reader = DirectoryReader.open(dir)) {
            final ProfileCollector stats = TestCollectors.elapsed();
            final ColumnarStringBinaryDocValues column = open(stats, reader.leaves().get(0).reader());
            while (column.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                column.binaryValue();
            }
            // NOTE: Metrics register when their reader is built, so this key set depends on what was built, not
            // on what the reads touched. The inflation metrics register inside ZstdBlockBytesCodec, which this
            // column does not build. A refactoring that moves a registration must keep the set.
            assertThat(
                TestCollectors.toKeyedValues(stats.snapshot()).keySet(),
                equalTo(
                    Set.of(
                        "ColumNAR.readers.opened",
                        "ColumNAR.readers.open_time_nanos",
                        "ColumNAR.readers.plain",
                        "ColumNAR.readers.dictionary",
                        "ColumNAR.readers.numeric",
                        "ColumNAR.pages.call_time_nanos",
                        "ColumNAR.pages.byte_lengths_call_time_nanos",
                        "ColumNAR.pages.as_ordinals",
                        "ColumNAR.pages.as_values",
                        "ColumNAR.pages.values_in_ordinal_pages",
                        "ColumNAR.pages.values_in_value_pages",
                        "ColumNAR.dictionary.term_lookups",
                        "ColumNAR.dictionary.resolve_time_nanos",
                        "ColumNAR.uncompressed.reads",
                        "ColumNAR.uncompressed.bytes_read",
                        "ColumNAR.uncompressed.read_time_nanos",
                        "ColumNAR.blocks.fetched",
                        "ColumNAR.blocks.bytes_fetched",
                        "ColumNAR.blocks.fetch_time_nanos",
                        "ColumNAR.blocks.decode_time_nanos",
                        "ColumNAR.blocks.values_decoded",
                        "ColumNAR.ordinal_skip.intervals_rejected",
                        "ColumNAR.ordinal_skip.docid_span",
                        "ColumNAR.ordinal_skip.navigation_time_nanos"
                    )
                )
            );
        }
    }

    private static ColumnarStringBinaryDocValues open(ProfileCollector stats, LeafReader leaf) throws IOException {
        try (Binding ignored = Scopes.bind(TestCollectors.eager(stats))) {
            return (ColumnarStringBinaryDocValues) leaf.getBinaryDocValues(FIELD);
        }
    }

    private static List<Integer> matchRange(ColumnarStringBinaryDocValues column, BytesRef lower, BytesRef upper) throws IOException {
        final DocIdSetIterator matches = column.reader().matchRange(lower, true, upper, true);
        final List<Integer> docs = new ArrayList<>();
        for (int doc = matches.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = matches.nextDoc()) {
            docs.add(doc);
        }
        return docs;
    }

    private static List<Integer> expected(String[] values, BytesRef lower, BytesRef upper) {
        final List<Integer> docs = new ArrayList<>();
        for (int doc = 0; doc < values.length; doc++) {
            final BytesRef value = new BytesRef(values[doc]);
            if (value.compareTo(lower) >= 0 && value.compareTo(upper) <= 0) {
                docs.add(doc);
            }
        }
        return docs;
    }

    private static Directory mergedColumn(StringColumnOptions options, String[] values) throws IOException {
        final Directory dir = newDirectory();
        final int segments = 3;
        final int perSegment = values.length / segments;
        try (IndexWriter writer = new IndexWriter(dir, writerConfig(NoMergePolicy.INSTANCE, options))) {
            for (int doc = 0; doc < values.length; doc++) {
                final Document document = new Document();
                document.add(new Field(FIELD, stringPayload(values[doc]), columnarBinaryFieldType()));
                writer.addDocument(document);
                if ((doc + 1) % perSegment == 0) {
                    writer.commit();
                }
            }
            writer.commit();
        }
        try (IndexWriter writer = new IndexWriter(dir, writerConfig(new LogDocMergePolicy(), options))) {
            writer.forceMerge(1);
        }
        return dir;
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

    private static String[] clustered(int count, int runs) {
        final String[] values = new String[count];
        final int perRun = (count + runs - 1) / runs;
        for (int doc = 0; doc < count; doc++) {
            values[doc] = String.format(Locale.ROOT, "term-%05d", (doc % perRun) * TERMS / perRun);
        }
        return values;
    }

    private static BytesRef term(int term) {
        return new BytesRef(String.format(Locale.ROOT, "term-%05d", term));
    }
}
