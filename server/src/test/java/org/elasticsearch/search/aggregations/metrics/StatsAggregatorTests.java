/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.metrics;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.mapper.NumberFieldMapper.NumberType;
import org.elasticsearch.script.MockScriptEngine;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptEngine;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.support.AggregationInspectionHelper;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.search.lookup.LeafDocLookup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.elasticsearch.search.aggregations.AggregationBuilders.stats;

public class StatsAggregatorTests extends AggregatorTestCase {

    private static final double TOLERANCE = 1e-10;
    private static final String VALUE_SCRIPT_NAME = "value_script";
    private static final String FIELD_SCRIPT_NAME = "field_script";

    // TODO: Script tests, should fail with defaultValuesSourceType disabled.

    public void testEmpty() throws IOException {
        final MappedFieldType ft = new NumberFieldMapper.NumberFieldType("field", NumberType.LONG);
        testCase(stats("_name").field(ft.name()), iw -> {}, stats -> {
            assertEquals(0d, stats.getCount(), 0);
            assertEquals(0d, stats.getSum(), 0);
            assertEquals(Float.NaN, stats.getAvg(), 0);
            assertEquals(Double.POSITIVE_INFINITY, stats.getMin(), 0);
            assertEquals(Double.NEGATIVE_INFINITY, stats.getMax(), 0);
            assertFalse(AggregationInspectionHelper.hasValue(stats));
        }, ft);
    }

    public void testRandomDoubles() throws IOException {
        final MappedFieldType ft = new NumberFieldMapper.NumberFieldType("field", NumberType.DOUBLE);
        final SimpleStatsAggregator expected = new SimpleStatsAggregator();
        testCase(stats("_name").field(ft.name()), iw -> {
            int numDocs = randomIntBetween(10, 50);
            for (int i = 0; i < numDocs; i++) {
                Document doc = new Document();
                int numValues = randomIntBetween(1, 5);
                for (int j = 0; j < numValues; j++) {
                    double value = randomDoubleBetween(-100d, 100d, true);
                    long valueAsLong = NumericUtils.doubleToSortableLong(value);
                    doc.add(new SortedNumericDocValuesField(ft.name(), valueAsLong));
                    expected.add(value);
                }
                iw.addDocument(doc);
            }
        }, stats -> {
            assertEquals(expected.count, stats.getCount(), 0);
            assertEquals(expected.sum, stats.getSum(), TOLERANCE);
            assertEquals(expected.min, stats.getMin(), 0);
            assertEquals(expected.max, stats.getMax(), 0);
            assertEquals(expected.sum / expected.count, stats.getAvg(), TOLERANCE);
            assertTrue(AggregationInspectionHelper.hasValue(stats));
        }, ft);
    }

    public void testRandomLongs() throws IOException {
        randomLongsTestCase(randomIntBetween(1, 5), stats("_name").field("field"), (expected, stats) -> {
            assertEquals(expected.count, stats.getCount(), 0);
            assertEquals(expected.sum, stats.getSum(), TOLERANCE);
            assertEquals(expected.min, stats.getMin(), 0);
            assertEquals(expected.max, stats.getMax(), 0);
            assertEquals(expected.sum / expected.count, stats.getAvg(), TOLERANCE);
            assertTrue(AggregationInspectionHelper.hasValue(stats));
        });
    }

    public void testSummationAccuracy() throws IOException {
        // Summing up a normal array and expect an accurate value
        double[] values = new double[] { 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7 };
        verifySummationOfDoubles(values, 15.3, 0.9, 0d, values.length * TOLERANCE);

        // Summing up an array which contains NaN and infinities and expect a result same as naive summation
        int n = randomIntBetween(5, 10);
        values = new double[n];
        double sum = 0;
        for (int i = 0; i < n; i++) {
            values[i] = frequently()
                ? randomFrom(Double.NaN, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY)
                : randomDoubleBetween(Double.MIN_VALUE, Double.MAX_VALUE, true);
            sum += values[i];
        }
        verifySummationOfDoubles(values, sum, sum / n, TOLERANCE, n * TOLERANCE);

        // Summing up some big double values and expect infinity result
        n = randomIntBetween(5, 10);
        double[] largeValues = new double[n];
        for (int i = 0; i < n; i++) {
            largeValues[i] = Double.MAX_VALUE;
        }
        verifySummationOfDoubles(largeValues, Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY, 0d, 0d);

        for (int i = 0; i < n; i++) {
            largeValues[i] = -Double.MAX_VALUE;
        }
        verifySummationOfDoubles(largeValues, Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY, 0d, 0d);
    }

    private void verifySummationOfDoubles(
        double[] values,
        double expectedSum,
        double expectedAvg,
        double singleSegmentDelta,
        double manySegmentDelta
    ) throws IOException {
        MappedFieldType ft = new NumberFieldMapper.NumberFieldType("field", NumberType.DOUBLE);

        double max = Double.NEGATIVE_INFINITY;
        double min = Double.POSITIVE_INFINITY;
        for (double value : values) {
            max = Math.max(max, value);
            min = Math.min(min, value);
        }
        double expectedMax = max;
        double expectedMin = min;
        testCase(stats("_name").field(ft.name()), iw -> {
            List<List<NumericDocValuesField>> docs = new ArrayList<>();
            for (double value : values) {
                docs.add(singletonList(new NumericDocValuesField(ft.name(), NumericUtils.doubleToSortableLong(value))));
            }
            iw.addDocuments(docs);
        }, stats -> {
            assertEquals(values.length, stats.getCount());
            assertEquals(expectedAvg, stats.getAvg(), singleSegmentDelta);
            assertEquals(expectedSum, stats.getSum(), singleSegmentDelta);
            assertEquals(expectedMax, stats.getMax(), 0d);
            assertEquals(expectedMin, stats.getMin(), 0d);
            assertTrue(AggregationInspectionHelper.hasValue(stats));
        }, ft);
        testCase(stats("_name").field(ft.name()), iw -> {
            for (double value : values) {
                iw.addDocument(singletonList(new NumericDocValuesField(ft.name(), NumericUtils.doubleToSortableLong(value))));
            }
        }, stats -> {
            assertEquals(values.length, stats.getCount());
            assertEquals(expectedAvg, stats.getAvg(), manySegmentDelta);
            assertEquals(expectedSum, stats.getSum(), manySegmentDelta);
            assertEquals(expectedMax, stats.getMax(), 0d);
            assertEquals(expectedMin, stats.getMin(), 0d);
            assertTrue(AggregationInspectionHelper.hasValue(stats));
        }, ft);
    }

    public void testUnmapped() throws IOException {
        randomLongsTestCase(randomIntBetween(1, 5), stats("_name").field("unmapped_field"), (expected, stats) -> {
            assertEquals(0d, stats.getCount(), 0);
            assertEquals(0d, stats.getSum(), 0);
            assertEquals(Float.NaN, stats.getAvg(), 0);
            assertEquals(Double.POSITIVE_INFINITY, stats.getMin(), 0);
            assertEquals(Double.NEGATIVE_INFINITY, stats.getMax(), 0);
            assertFalse(AggregationInspectionHelper.hasValue(stats));
        });
    }

    public void testPartiallyUnmapped() throws IOException {
        try (
            Directory mappedDirectory = newDirectory();
            Directory unmappedDirectory = newDirectory();
            RandomIndexWriter mappedWriter = new RandomIndexWriter(random(), mappedDirectory);
            RandomIndexWriter unmappedWriter = new RandomIndexWriter(random(), unmappedDirectory)
        ) {

            final MappedFieldType ft = new NumberFieldMapper.NumberFieldType("field", NumberType.LONG);
            final SimpleStatsAggregator expected = new SimpleStatsAggregator();
            final int numDocs = randomIntBetween(10, 50);
            for (int i = 0; i < numDocs; i++) {
                final long value = randomLongBetween(-100, 100);
                mappedWriter.addDocument(singleton(new SortedNumericDocValuesField(ft.name(), value)));
                expected.add(value);
            }
            final StatsAggregationBuilder builder = stats("_name").field(ft.name());

            try (
                IndexReader mappedReader = mappedWriter.getReader();
                IndexReader unmappedReader = unmappedWriter.getReader();
                MultiReader multiReader = new MultiReader(mappedReader, unmappedReader)
            ) {

                final IndexSearcher searcher = new IndexSearcher(multiReader);
                final InternalStats stats = searchAndReduce(searcher, new MatchAllDocsQuery(), builder, ft);

                assertEquals(expected.count, stats.getCount(), 0);
                assertEquals(expected.sum, stats.getSum(), TOLERANCE);
                assertEquals(expected.max, stats.getMax(), 0);
                assertEquals(expected.min, stats.getMin(), 0);
                assertEquals(expected.sum / expected.count, stats.getAvg(), TOLERANCE);
                assertTrue(AggregationInspectionHelper.hasValue(stats));
            }
        }
    }

    public void testValueScriptSingleValuedField() throws IOException {
        randomLongsTestCase(
            1,
            stats("_name").field("field").script(new Script(ScriptType.INLINE, MockScriptEngine.NAME, VALUE_SCRIPT_NAME, emptyMap())),
            (expected, stats) -> {
                final SimpleStatsAggregator adjusted = new SimpleStatsAggregator(
                    expected.count,
                    expected.min + 1,
                    expected.max + 1,
                    expected.sum + expected.count
                );

                assertEquals(adjusted.count, stats.getCount(), 0);
                assertEquals(adjusted.sum, stats.getSum(), TOLERANCE);
                assertEquals(adjusted.max, stats.getMax(), 0);
                assertEquals(adjusted.min, stats.getMin(), 0);
                assertEquals(adjusted.sum / adjusted.count, stats.getAvg(), TOLERANCE);
                assertTrue(AggregationInspectionHelper.hasValue(stats));
            }
        );
    }

    public void testValueScriptMultiValuedField() throws IOException {
        final int valuesPerField = randomIntBetween(2, 5);
        randomLongsTestCase(
            valuesPerField,
            stats("_name").field("field").script(new Script(ScriptType.INLINE, MockScriptEngine.NAME, VALUE_SCRIPT_NAME, emptyMap())),
            (expected, stats) -> {
                final SimpleStatsAggregator adjusted = new SimpleStatsAggregator(
                    expected.count,
                    expected.min + 1,
                    expected.max + 1,
                    expected.sum + expected.count
                );

                assertEquals(adjusted.count, stats.getCount(), 0);
                assertEquals(adjusted.sum, stats.getSum(), TOLERANCE);
                assertEquals(adjusted.max, stats.getMax(), 0);
                assertEquals(adjusted.min, stats.getMin(), 0);
                assertEquals(adjusted.sum / adjusted.count, stats.getAvg(), TOLERANCE);
                assertTrue(AggregationInspectionHelper.hasValue(stats));
            }
        );
    }

    public void testFieldScriptSingleValuedField() throws IOException {
        randomLongsTestCase(
            1,
            stats("_name").script(new Script(ScriptType.INLINE, MockScriptEngine.NAME, FIELD_SCRIPT_NAME, singletonMap("field", "field"))),
            (expected, stats) -> {
                final SimpleStatsAggregator adjusted = new SimpleStatsAggregator(
                    expected.count,
                    expected.min + 1,
                    expected.max + 1,
                    expected.sum + expected.count
                );

                assertEquals(adjusted.count, stats.getCount(), 0);
                assertEquals(adjusted.sum, stats.getSum(), TOLERANCE);
                assertEquals(adjusted.max, stats.getMax(), 0);
                assertEquals(adjusted.min, stats.getMin(), 0);
                assertEquals(adjusted.sum / adjusted.count, stats.getAvg(), TOLERANCE);
                assertTrue(AggregationInspectionHelper.hasValue(stats));
            }
        );
    }

    public void testFieldScriptMultiValuedField() throws IOException {
        final int valuesPerField = randomIntBetween(2, 5);
        randomLongsTestCase(
            valuesPerField,
            stats("_name").script(new Script(ScriptType.INLINE, MockScriptEngine.NAME, FIELD_SCRIPT_NAME, singletonMap("field", "field"))),
            (expected, stats) -> {
                final SimpleStatsAggregator adjusted = new SimpleStatsAggregator(
                    expected.count,
                    expected.min + 1,
                    expected.max + 1,
                    expected.sum + expected.count
                );

                assertEquals(adjusted.count, stats.getCount(), 0);
                assertEquals(adjusted.sum, stats.getSum(), TOLERANCE);
                assertEquals(adjusted.max, stats.getMax(), 0);
                assertEquals(adjusted.min, stats.getMin(), 0);
                assertEquals(adjusted.sum / adjusted.count, stats.getAvg(), TOLERANCE);
                assertTrue(AggregationInspectionHelper.hasValue(stats));
            }
        );
    }

    public void testMissing() throws IOException {
        final MappedFieldType ft = new NumberFieldMapper.NumberFieldType("field", NumberType.LONG);

        final long missingValue = randomIntBetween(-100, 100);

        final int numDocs = randomIntBetween(100, 200);
        final List<Set<IndexableField>> docs = new ArrayList<>(numDocs);
        final SimpleStatsAggregator expected = new SimpleStatsAggregator();
        for (int i = 0; i < numDocs; i++) {
            if (randomBoolean()) {
                final long value = randomLongBetween(-100, 100);
                docs.add(singleton(new SortedNumericDocValuesField(ft.name(), value)));
                expected.add(value);
            } else {
                docs.add(emptySet());
                expected.add(missingValue);
            }
        }

        testCase(stats("_name").field(ft.name()).missing(missingValue), iw -> iw.addDocuments(docs), stats -> {
            assertEquals(expected.count, stats.getCount(), 0);
            assertEquals(expected.sum, stats.getSum(), TOLERANCE);
            assertEquals(expected.max, stats.getMax(), 0);
            assertEquals(expected.min, stats.getMin(), 0);
            assertEquals(expected.sum / expected.count, stats.getAvg(), TOLERANCE);
            assertTrue(AggregationInspectionHelper.hasValue(stats));
        }, ft);
    }

    public void testMissingUnmapped() throws IOException {
        final int valuesPerField = randomIntBetween(1, 5);
        final long missingValue = randomLongBetween(-100, 100);
        randomLongsTestCase(valuesPerField, stats("_name").field("unknown_field").missing(missingValue), (expected, stats) -> {
            final long numDocs = expected.count / valuesPerField;
            assertEquals(numDocs, stats.getCount());
            assertEquals(numDocs * missingValue, stats.getSum(), TOLERANCE);
            assertEquals(missingValue, stats.getMax(), 0);
            assertEquals(missingValue, stats.getMin(), 0);
            assertEquals(missingValue, stats.getAvg(), TOLERANCE);
            assertTrue(AggregationInspectionHelper.hasValue(stats));
        });
    }

    private void randomLongsTestCase(
        int valuesPerField,
        StatsAggregationBuilder builder,
        BiConsumer<SimpleStatsAggregator, InternalStats> verify
    ) throws IOException {

        final MappedFieldType ft = new NumberFieldMapper.NumberFieldType("field", NumberType.LONG);

        final int numDocs = randomIntBetween(10, 50);
        final List<Set<IndexableField>> docs = new ArrayList<>(numDocs);
        final SimpleStatsAggregator expected = new SimpleStatsAggregator();
        for (int iDoc = 0; iDoc < numDocs; iDoc++) {
            List<Long> values = randomList(valuesPerField, valuesPerField, () -> randomLongBetween(-100, 100));
            docs.add(values.stream().map(value -> new SortedNumericDocValuesField(ft.name(), value)).collect(toSet()));
            values.forEach(expected::add);
        }

        testCase(builder, iw -> iw.addDocuments(docs), stats -> verify.accept(expected, stats), ft);
    }

    private void testCase(
        StatsAggregationBuilder builder,
        CheckedConsumer<RandomIndexWriter, IOException> buildIndex,
        Consumer<InternalStats> verify,
        MappedFieldType... fieldTypes
    ) throws IOException {
        testCase(builder, new MatchAllDocsQuery(), buildIndex, verify, fieldTypes);
    }

    static class SimpleStatsAggregator {
        long count = 0;
        double min = Long.MAX_VALUE;
        double max = Long.MIN_VALUE;
        double sum = 0;

        SimpleStatsAggregator() {}

        SimpleStatsAggregator(long count, double min, double max, double sum) {
            this.count = count;
            this.min = min;
            this.max = max;
            this.sum = sum;
        }

        void add(double value) {
            count++;
            if (Double.compare(value, min) < 0) {
                min = value;
            }
            if (Double.compare(value, max) > 0) {
                max = value;
            }
            sum += value;
        }
    }

    @Override
    protected List<ValuesSourceType> getSupportedValuesSourceTypes() {
        return List.of(CoreValuesSourceType.NUMERIC, CoreValuesSourceType.BOOLEAN, CoreValuesSourceType.DATE);
    }

    @Override
    protected AggregationBuilder createAggBuilderForTypeTest(MappedFieldType fieldType, String fieldName) {
        return new StatsAggregationBuilder("_name").field(fieldName);
    }

    @Override
    protected ScriptService getMockScriptService() {
        final Map<String, Function<Map<String, Object>, Object>> scripts = Map.of(
            VALUE_SCRIPT_NAME,
            vars -> ((Number) vars.get("_value")).doubleValue() + 1,
            FIELD_SCRIPT_NAME,
            vars -> {
                final String fieldName = (String) vars.get("field");
                final LeafDocLookup lookup = (LeafDocLookup) vars.get("doc");
                return lookup.get(fieldName).stream().map(value -> ((Number) value).longValue() + 1).collect(toList());
            }
        );
        final MockScriptEngine engine = new MockScriptEngine(MockScriptEngine.NAME, scripts, emptyMap());
        final Map<String, ScriptEngine> engines = singletonMap(engine.getType(), engine);
        return new ScriptService(Settings.EMPTY, engines, ScriptModule.CORE_CONTEXTS, () -> 1L);
    }
}
