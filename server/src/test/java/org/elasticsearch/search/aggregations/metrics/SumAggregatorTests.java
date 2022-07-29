/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.metrics;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.common.TriConsumer;
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
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.toList;
import static org.elasticsearch.search.aggregations.AggregationBuilders.sum;

public class SumAggregatorTests extends AggregatorTestCase {

    private static final String FIELD_NAME = "field";
    private static final String VALUE_SCRIPT_NAME = "value_script";
    private static final String FIELD_SCRIPT_NAME = "field_script";

    public void testNoDocs() throws IOException {
        testAggregation(new MatchAllDocsQuery(), iw -> {
            // Intentionally not writing any docs
        }, count -> {
            assertEquals(0L, count.value(), 0d);
            assertFalse(AggregationInspectionHelper.hasValue(count));
        });
    }

    public void testNoMatchingField() throws IOException {
        testAggregation(new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new NumericDocValuesField("wrong_number", 7)));
            iw.addDocument(singleton(new NumericDocValuesField("wrong_number", 1)));
        }, count -> {
            assertEquals(0L, count.value(), 0d);
            assertFalse(AggregationInspectionHelper.hasValue(count));
        });
    }

    public void testNumericDocValues() throws IOException {
        testAggregation(new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new NumericDocValuesField(FIELD_NAME, 1)));
            iw.addDocument(singleton(new NumericDocValuesField(FIELD_NAME, 2)));
            iw.addDocument(singleton(new NumericDocValuesField(FIELD_NAME, 1)));
            iw.addDocument(singleton(new NumericDocValuesField(FIELD_NAME, 2)));
            iw.addDocument(singleton(new NumericDocValuesField(FIELD_NAME, 1)));
            iw.addDocument(singleton(new NumericDocValuesField(FIELD_NAME, 2)));
            iw.addDocument(singleton(new NumericDocValuesField(FIELD_NAME, 1)));
            iw.addDocument(singleton(new NumericDocValuesField(FIELD_NAME, 2)));
            iw.addDocument(singleton(new NumericDocValuesField(FIELD_NAME, 1)));
            iw.addDocument(singleton(new NumericDocValuesField(FIELD_NAME, 2)));
            iw.addDocument(singleton(new NumericDocValuesField(FIELD_NAME, 1)));
            iw.addDocument(singleton(new NumericDocValuesField(FIELD_NAME, 2)));
            iw.addDocument(singleton(new NumericDocValuesField(FIELD_NAME, 1)));
            iw.addDocument(singleton(new NumericDocValuesField(FIELD_NAME, 2)));
            iw.addDocument(singleton(new NumericDocValuesField(FIELD_NAME, 1)));
            iw.addDocument(singleton(new NumericDocValuesField(FIELD_NAME, 2)));
        }, count -> {
            assertEquals(24L, count.value(), 0d);
            assertTrue(AggregationInspectionHelper.hasValue(count));
        });
    }

    public void testSortedNumericDocValues() throws IOException {
        testAggregation(new FieldExistsQuery(FIELD_NAME), iw -> {
            iw.addDocument(Arrays.asList(new SortedNumericDocValuesField(FIELD_NAME, 3), new SortedNumericDocValuesField(FIELD_NAME, 4)));
            iw.addDocument(Arrays.asList(new SortedNumericDocValuesField(FIELD_NAME, 3), new SortedNumericDocValuesField(FIELD_NAME, 4)));
            iw.addDocument(singleton(new SortedNumericDocValuesField(FIELD_NAME, 1)));
        }, count -> {
            assertEquals(15L, count.value(), 0d);
            assertTrue(AggregationInspectionHelper.hasValue(count));
        });
    }

    public void testQueryFiltering() throws IOException {
        testAggregation(new TermQuery(new Term("match", "yes")), iw -> {
            iw.addDocument(Arrays.asList(new StringField("match", "yes", Field.Store.NO), new NumericDocValuesField(FIELD_NAME, 1)));
            iw.addDocument(Arrays.asList(new StringField("match", "no", Field.Store.NO), new NumericDocValuesField(FIELD_NAME, 2)));
            iw.addDocument(Arrays.asList(new StringField("match", "yes", Field.Store.NO), new NumericDocValuesField(FIELD_NAME, 3)));
            iw.addDocument(Arrays.asList(new StringField("match", "no", Field.Store.NO), new NumericDocValuesField(FIELD_NAME, 4)));
            iw.addDocument(Arrays.asList(new StringField("match", "yes", Field.Store.NO), new NumericDocValuesField(FIELD_NAME, 5)));
        }, count -> {
            assertEquals(9L, count.value(), 0d);
            assertTrue(AggregationInspectionHelper.hasValue(count));
        });
    }

    public void testStringField() throws IOException {
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> {
            testAggregation(
                new MatchAllDocsQuery(),
                iw -> { iw.addDocument(singleton(new SortedDocValuesField(FIELD_NAME, new BytesRef("1")))); },
                count -> {
                    assertEquals(0L, count.value(), 0d);
                    assertFalse(AggregationInspectionHelper.hasValue(count));
                }
            );
        });
        assertEquals(
            "unexpected docvalues type SORTED for field 'field' (expected one of [SORTED_NUMERIC, NUMERIC]). "
                + "Re-index with correct docvalues type.",
            e.getMessage()
        );
    }

    public void testSummationAccuracy() throws IOException {
        // Summing up a normal array and expect an accurate value
        double[] values = new double[] { 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7 };
        verifySummationOfDoubles(values, 15.3, Double.MIN_NORMAL);

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
        verifySummationOfDoubles(values, sum, 1e-10);

        // Summing up some big double values and expect infinity result
        n = randomIntBetween(5, 10);
        double[] largeValues = new double[n];
        for (int i = 0; i < n; i++) {
            largeValues[i] = Double.MAX_VALUE;
        }
        verifySummationOfDoubles(largeValues, Double.POSITIVE_INFINITY, 0d);

        for (int i = 0; i < n; i++) {
            largeValues[i] = -Double.MAX_VALUE;
        }
        verifySummationOfDoubles(largeValues, Double.NEGATIVE_INFINITY, 0d);
    }

    private void verifySummationOfDoubles(double[] values, double expected, double delta) throws IOException {
        testAggregation(sum("_name").field(FIELD_NAME), new MatchAllDocsQuery(), iw -> {
            /*
             * The sum agg uses a Kahan sumation on the shard to limit
             * floating point errors. But it doesn't ship the sums to the
             * coordinating node, so floaing point error can creep in when
             * reducing many sums. The test framework aggregates each
             * segment as though it were a separate shard, then reduces
             * those togther. Fun. But it means we don't get the full
             * accuracy of the Kahan sumation. And *that* accuracy is
             * what this method is trying to test. So we have to stick
             * all the documents on the same leaf. `addDocuments` does
             * that.
             */
            iw.addDocuments(
                Arrays.stream(values)
                    .mapToObj(value -> singleton(new NumericDocValuesField(FIELD_NAME, NumericUtils.doubleToSortableLong(value))))
                    .collect(toList())
            );
        }, result -> assertEquals(expected, result.value(), delta), defaultFieldType(NumberType.DOUBLE));
    }

    public void testUnmapped() throws IOException {
        sumRandomDocsTestCase(randomIntBetween(1, 5), sum("_name").field("unknown_field"), (sum, docs, result) -> {
            assertEquals(0d, result.value(), 0d);
            assertFalse(AggregationInspectionHelper.hasValue(result));
        });
    }

    public void testPartiallyUnmapped() throws IOException {
        final MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType(FIELD_NAME, NumberType.LONG);

        final SumAggregationBuilder builder = sum("_name").field(fieldType.name());

        final int numDocs = randomIntBetween(10, 100);
        final List<Set<IndexableField>> docs = new ArrayList<>(numDocs);
        int sum = 0;
        for (int i = 0; i < numDocs; i++) {
            final long value = randomLongBetween(0, 1000);
            sum += value;
            docs.add(singleton(new NumericDocValuesField(fieldType.name(), value)));
        }

        try (Directory mappedDirectory = newDirectory(); Directory unmappedDirectory = newDirectory()) {
            try (RandomIndexWriter mappedWriter = new RandomIndexWriter(random(), mappedDirectory)) {
                mappedWriter.addDocuments(docs);
            }

            new RandomIndexWriter(random(), unmappedDirectory).close();

            try (
                IndexReader mappedReader = DirectoryReader.open(mappedDirectory);
                IndexReader unmappedReader = DirectoryReader.open(unmappedDirectory);
                MultiReader multiReader = new MultiReader(mappedReader, unmappedReader)
            ) {

                final IndexSearcher searcher = newSearcher(multiReader, true, true);

                final Sum internalSum = searchAndReduce(searcher, new MatchAllDocsQuery(), builder, fieldType);
                assertEquals(sum, internalSum.value(), 0d);
                assertTrue(AggregationInspectionHelper.hasValue(internalSum));
            }
        }
    }

    public void testValueScriptSingleValuedField() throws IOException {
        sumRandomDocsTestCase(
            1,
            sum("_name").field(FIELD_NAME).script(new Script(ScriptType.INLINE, MockScriptEngine.NAME, VALUE_SCRIPT_NAME, emptyMap())),
            (sum, docs, result) -> {
                assertEquals(sum + docs.size(), result.value(), 0d);
                assertTrue(AggregationInspectionHelper.hasValue(result));
            }
        );
    }

    public void testValueScriptMultiValuedField() throws IOException {
        final int valuesPerField = randomIntBetween(2, 5);
        sumRandomDocsTestCase(
            valuesPerField,
            sum("_name").field(FIELD_NAME).script(new Script(ScriptType.INLINE, MockScriptEngine.NAME, VALUE_SCRIPT_NAME, emptyMap())),
            (sum, docs, result) -> {
                assertEquals(sum + (docs.size() * valuesPerField), result.value(), 0d);
                assertTrue(AggregationInspectionHelper.hasValue(result));
            }
        );
    }

    public void testFieldScriptSingleValuedField() throws IOException {
        sumRandomDocsTestCase(
            1,
            sum("_name").script(new Script(ScriptType.INLINE, MockScriptEngine.NAME, FIELD_SCRIPT_NAME, singletonMap("field", FIELD_NAME))),
            (sum, docs, result) -> {
                assertEquals(sum + docs.size(), result.value(), 0d);
                assertTrue(AggregationInspectionHelper.hasValue(result));
            }
        );
    }

    public void testFieldScriptMultiValuedField() throws IOException {
        final int valuesPerField = randomIntBetween(2, 5);
        sumRandomDocsTestCase(
            valuesPerField,
            sum("_name").script(new Script(ScriptType.INLINE, MockScriptEngine.NAME, FIELD_SCRIPT_NAME, singletonMap("field", FIELD_NAME))),
            (sum, docs, result) -> {
                assertEquals(sum + (docs.size() * valuesPerField), result.value(), 0d);
                assertTrue(AggregationInspectionHelper.hasValue(result));
            }
        );
    }

    public void testMissing() throws IOException {
        final MappedFieldType aggField = defaultFieldType();
        final MappedFieldType irrelevantField = new NumberFieldMapper.NumberFieldType("irrelevant_field", NumberType.LONG);

        final int numDocs = randomIntBetween(10, 100);
        final long missingValue = randomLongBetween(1, 1000);
        long sum = 0;
        final List<Set<IndexableField>> docs = new ArrayList<>(numDocs);
        for (int i = 0; i < numDocs; i++) {
            if (randomBoolean()) {
                final long value = randomLongBetween(0, 1000);
                sum += value;
                docs.add(singleton(new NumericDocValuesField(aggField.name(), value)));
            } else {
                sum += missingValue;
                docs.add(singleton(new NumericDocValuesField(irrelevantField.name(), randomLong())));
            }
        }
        final long finalSum = sum;

        testAggregation(
            sum("_name").field(aggField.name()).missing(missingValue),
            new MatchAllDocsQuery(),
            writer -> writer.addDocuments(docs),
            internalSum -> {
                assertEquals(finalSum, internalSum.value(), 0d);
                assertTrue(AggregationInspectionHelper.hasValue(internalSum));
            },
            aggField,
            irrelevantField
        );
    }

    public void testMissingUnmapped() throws IOException {
        final long missingValue = randomLongBetween(1, 1000);
        sumRandomDocsTestCase(randomIntBetween(1, 5), sum("_name").field("unknown_field").missing(missingValue), (sum, docs, result) -> {
            assertEquals(docs.size() * missingValue, result.value(), 0d);
            assertTrue(AggregationInspectionHelper.hasValue(result));
        });
    }

    private void sumRandomDocsTestCase(
        int valuesPerField,
        SumAggregationBuilder builder,
        TriConsumer<Long, List<Set<IndexableField>>, Sum> verify
    ) throws IOException {

        final MappedFieldType fieldType = defaultFieldType();

        final int numDocs = randomIntBetween(10, 100);
        final List<Set<IndexableField>> docs = new ArrayList<>(numDocs);
        long sum = 0;
        for (int iDoc = 0; iDoc < numDocs; iDoc++) {
            Set<IndexableField> doc = new HashSet<>();
            for (int iValue = 0; iValue < valuesPerField; iValue++) {
                final long value = randomLongBetween(0, 1000);
                sum += value;
                doc.add(new SortedNumericDocValuesField(fieldType.name(), value));
            }
            docs.add(doc);
        }
        final long finalSum = sum;

        testAggregation(
            builder,
            new MatchAllDocsQuery(),
            writer -> writer.addDocuments(docs),
            internalSum -> verify.apply(finalSum, docs, internalSum),
            fieldType
        );
    }

    private void testAggregation(Query query, CheckedConsumer<RandomIndexWriter, IOException> indexer, Consumer<Sum> verify)
        throws IOException {
        AggregationBuilder aggregationBuilder = sum("_name").field(FIELD_NAME);
        testAggregation(aggregationBuilder, query, indexer, verify, defaultFieldType());
    }

    private void testAggregation(
        AggregationBuilder aggregationBuilder,
        Query query,
        CheckedConsumer<RandomIndexWriter, IOException> indexer,
        Consumer<Sum> verify,
        MappedFieldType... fieldTypes
    ) throws IOException {
        testCase(aggregationBuilder, query, indexer, verify, fieldTypes);
    }

    @Override
    protected List<ValuesSourceType> getSupportedValuesSourceTypes() {
        return List.of(CoreValuesSourceType.NUMERIC, CoreValuesSourceType.BOOLEAN, CoreValuesSourceType.DATE);
    }

    @Override
    protected AggregationBuilder createAggBuilderForTypeTest(MappedFieldType fieldType, String fieldName) {
        return new SumAggregationBuilder("_name").field(fieldName);
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

    private static MappedFieldType defaultFieldType() {
        return defaultFieldType(NumberType.LONG);
    }

    private static MappedFieldType defaultFieldType(NumberType numberType) {
        return new NumberFieldMapper.NumberFieldType(FIELD_NAME, numberType);
    }
}
