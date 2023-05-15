/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.metrics;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.Bits;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.script.MockScriptEngine;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptEngine;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.BucketCollector;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.MultiBucketCollector;
import org.elasticsearch.search.aggregations.bucket.filter.Filter;
import org.elasticsearch.search.aggregations.bucket.global.Global;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.AggregationInspectionHelper;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.search.lookup.LeafDocLookup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.util.Collections.emptyList;
import static java.util.Collections.singleton;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.hamcrest.Matchers.equalTo;

public class MaxAggregatorTests extends AggregatorTestCase {

    private static final String SCRIPT_NAME = "script_name";

    private static final long SCRIPT_VALUE = 19L;

    /** Script to take a field name in params and sum the values of the field. */
    public static final String SUM_FIELD_PARAMS_SCRIPT = "sum_field_params";

    /** Script to sum the values of a field named {@code values}. */
    public static final String SUM_VALUES_FIELD_SCRIPT = "sum_values_field";

    /** Script to return the value of a field named {@code value}. */
    public static final String VALUE_FIELD_SCRIPT = "value_field";

    /** Script to return the {@code _value} provided by aggs framework. */
    public static final String VALUE_SCRIPT = "_value";

    /** Script to return a random double */
    public static final String RANDOM_SCRIPT = "Math.random()";

    @Override
    protected ScriptService getMockScriptService() {
        Map<String, Function<Map<String, Object>, Object>> scripts = new HashMap<>();
        Function<Map<String, Object>, Integer> getInc = vars -> {
            if (vars == null || vars.containsKey("inc") == false) {
                return 0;
            } else {
                return ((Number) vars.get("inc")).intValue();
            }
        };

        BiFunction<Map<String, Object>, String, Object> sum = (vars, fieldname) -> {
            int inc = getInc.apply(vars);
            LeafDocLookup docLookup = (LeafDocLookup) vars.get("doc");
            List<Long> values = new ArrayList<>();
            for (Object v : docLookup.get(fieldname)) {
                values.add(((Number) v).longValue() + inc);
            }
            return values;
        };

        scripts.put(SCRIPT_NAME, script -> SCRIPT_VALUE);
        scripts.put(SUM_FIELD_PARAMS_SCRIPT, vars -> {
            String fieldname = (String) vars.get("field");
            return sum.apply(vars, fieldname);
        });
        scripts.put(SUM_VALUES_FIELD_SCRIPT, vars -> sum.apply(vars, "values"));
        scripts.put(VALUE_FIELD_SCRIPT, vars -> sum.apply(vars, "value"));
        scripts.put(VALUE_SCRIPT, vars -> {
            int inc = getInc.apply(vars);
            return ((Number) vars.get("_value")).doubleValue() + inc;
        });

        Map<String, Function<Map<String, Object>, Object>> nonDeterministicScripts = new HashMap<>();
        nonDeterministicScripts.put(RANDOM_SCRIPT, vars -> MaxAggregatorTests.randomDouble());

        MockScriptEngine scriptEngine = new MockScriptEngine(
            MockScriptEngine.NAME,
            scripts,
            nonDeterministicScripts,
            Collections.emptyMap()
        );
        Map<String, ScriptEngine> engines = Collections.singletonMap(scriptEngine.getType(), scriptEngine);

        return new ScriptService(Settings.EMPTY, engines, ScriptModule.CORE_CONTEXTS, () -> 1L);
    }

    @Override
    protected List<ValuesSourceType> getSupportedValuesSourceTypes() {
        return List.of(CoreValuesSourceType.NUMERIC, CoreValuesSourceType.BOOLEAN, CoreValuesSourceType.DATE);
    }

    @Override
    protected AggregationBuilder createAggBuilderForTypeTest(MappedFieldType fieldType, String fieldName) {
        return new MaxAggregationBuilder("_name").field(fieldName);
    }

    public void testNoDocs() throws IOException {
        testAggregation(new MatchAllDocsQuery(), iw -> {
            // Intentionally not writing any docs
        }, max -> {
            assertEquals(Double.NEGATIVE_INFINITY, max.value(), 0);
            assertFalse(AggregationInspectionHelper.hasValue(max));
        });
    }

    public void testNoMatchingField() throws IOException {
        testAggregation(new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new SortedNumericDocValuesField("wrong_number", 7)));
            iw.addDocument(singleton(new SortedNumericDocValuesField("wrong_number", 1)));
        }, max -> {
            assertEquals(Double.NEGATIVE_INFINITY, max.value(), 0);
            assertFalse(AggregationInspectionHelper.hasValue(max));
        });
    }

    public void testSomeMatchesSortedNumericDocValues() throws IOException {
        testAggregation(new FieldExistsQuery("number"), iw -> {
            iw.addDocument(singleton(new SortedNumericDocValuesField("number", 7)));
            iw.addDocument(singleton(new SortedNumericDocValuesField("number", 1)));
        }, max -> {
            assertEquals(7, max.value(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(max));
        });
    }

    public void testSomeMatchesNumericDocValues() throws IOException {
        testAggregation(new FieldExistsQuery("number"), iw -> {
            iw.addDocument(singleton(new NumericDocValuesField("number", 7)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 1)));
        }, max -> {
            assertEquals(7, max.value(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(max));
        });
    }

    public void testQueryFiltering() throws IOException {
        testAggregation(IntPoint.newRangeQuery("number", 0, 5), iw -> {
            iw.addDocument(Arrays.asList(new IntPoint("number", 7), new SortedNumericDocValuesField("number", 7)));
            iw.addDocument(Arrays.asList(new IntPoint("number", 1), new SortedNumericDocValuesField("number", 1)));
        }, max -> {
            assertEquals(1, max.value(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(max));
        });
    }

    public void testQueryFiltersAll() throws IOException {
        testAggregation(IntPoint.newRangeQuery("number", -1, 0), iw -> {
            iw.addDocument(Arrays.asList(new IntPoint("number", 7), new SortedNumericDocValuesField("number", 7)));
            iw.addDocument(Arrays.asList(new IntPoint("number", 1), new SortedNumericDocValuesField("number", 1)));
        }, max -> {
            assertEquals(Double.NEGATIVE_INFINITY, max.value(), 0);
            assertFalse(AggregationInspectionHelper.hasValue(max));
        });
    }

    public void testUnmappedField() throws IOException {
        MaxAggregationBuilder aggregationBuilder = new MaxAggregationBuilder("_name").field("number");
        testAggregation(aggregationBuilder, new FieldExistsQuery("number"), iw -> {
            iw.addDocument(singleton(new NumericDocValuesField("number", 7)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 1)));
        }, max -> {
            assertEquals(max.value(), Double.NEGATIVE_INFINITY, 0);
            assertFalse(AggregationInspectionHelper.hasValue(max));
        });
    }

    public void testUnmappedWithMissingField() throws IOException {
        MaxAggregationBuilder aggregationBuilder = new MaxAggregationBuilder("_name").field("number").missing(19L);

        testAggregation(aggregationBuilder, new FieldExistsQuery("number"), iw -> {
            iw.addDocument(singleton(new NumericDocValuesField("number", 7)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 1)));
        }, max -> {
            assertEquals(max.value(), 19.0, 0);
            assertTrue(AggregationInspectionHelper.hasValue(max));
        });
    }

    public void testMissingFieldOptimization() throws IOException {
        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("number", NumberFieldMapper.NumberType.INTEGER);

        AggregationBuilder aggregationBuilder = new MaxAggregationBuilder("_name").field("number").missing(19L);
        testAggregation(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            iw.addDocument(Arrays.asList(new IntPoint("number", 7), new SortedNumericDocValuesField("number", 7)));
            iw.addDocument(Arrays.asList(new IntPoint("number", 1), new SortedNumericDocValuesField("number", 1)));
            iw.addDocument(emptyList());
        }, max -> {
            assertEquals(max.value(), 19.0, 0);
            assertTrue(AggregationInspectionHelper.hasValue(max));
        }, fieldType);
    }

    public void testScript() throws IOException {
        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("number", NumberFieldMapper.NumberType.INTEGER);

        AggregationBuilder aggregationBuilder = new MaxAggregationBuilder("_name").field("number")
            .script(new Script(ScriptType.INLINE, MockScriptEngine.NAME, SCRIPT_NAME, Collections.emptyMap()));

        testAggregation(aggregationBuilder, new FieldExistsQuery("number"), iw -> {
            iw.addDocument(singleton(new NumericDocValuesField("number", 7)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 1)));
        }, max -> {
            assertEquals(max.value(), SCRIPT_VALUE, 0); // Note this is the script value (19L), not the doc values above
            assertTrue(AggregationInspectionHelper.hasValue(max));
        }, fieldType);
    }

    private void testAggregation(Query query, CheckedConsumer<RandomIndexWriter, IOException> buildIndex, Consumer<Max> verify)
        throws IOException {
        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("number", NumberFieldMapper.NumberType.INTEGER);
        MaxAggregationBuilder aggregationBuilder = new MaxAggregationBuilder("_name").field("number");
        testAggregation(aggregationBuilder, query, buildIndex, verify, fieldType);
    }

    private void testAggregation(
        AggregationBuilder aggregationBuilder,
        Query query,
        CheckedConsumer<RandomIndexWriter, IOException> buildIndex,
        Consumer<Max> verify,
        MappedFieldType... fieldTypes
    ) throws IOException {
        testCase(buildIndex, verify, new AggTestConfig(aggregationBuilder, fieldTypes).withQuery(query));
    }

    public void testMaxShortcutRandom() throws Exception {
        testMaxShortcutCase(
            () -> randomLongBetween(Integer.MIN_VALUE, Integer.MAX_VALUE),
            (n) -> new LongPoint("number", n.longValue()),
            (v) -> LongPoint.decodeDimension(v, 0)
        );

        testMaxShortcutCase(() -> randomInt(), (n) -> new IntPoint("number", n.intValue()), (v) -> IntPoint.decodeDimension(v, 0));

        testMaxShortcutCase(() -> randomFloat(), (n) -> new FloatPoint("number", n.floatValue()), (v) -> FloatPoint.decodeDimension(v, 0));

        testMaxShortcutCase(
            () -> randomDouble(),
            (n) -> new DoublePoint("number", n.doubleValue()),
            (v) -> DoublePoint.decodeDimension(v, 0)
        );
    }

    private void testMaxShortcutCase(
        Supplier<Number> randomNumber,
        Function<Number, Field> pointFieldFunc,
        Function<byte[], Number> pointConvertFunc
    ) throws IOException {
        Directory directory = newDirectory();
        IndexWriterConfig config = newIndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE);
        IndexWriter indexWriter = new IndexWriter(directory, config);
        List<Document> documents = new ArrayList<>();
        List<Tuple<Integer, Number>> values = new ArrayList<>();
        int numValues = atLeast(50);
        int docID = 0;
        for (int i = 0; i < numValues; i++) {
            int numDup = randomIntBetween(1, 3);
            for (int j = 0; j < numDup; j++) {
                Document document = new Document();
                Number nextValue = randomNumber.get();
                values.add(new Tuple<>(docID, nextValue));
                document.add(new StringField("id", Integer.toString(docID), Field.Store.NO));
                document.add(pointFieldFunc.apply(nextValue));
                documents.add(document);
                docID++;
            }
        }
        // insert some documents without a value for the metric field.
        for (int i = 0; i < 3; i++) {
            Document document = new Document();
            documents.add(document);
        }
        indexWriter.addDocuments(documents);
        Collections.sort(values, Comparator.comparingDouble(t -> t.v2().doubleValue()));
        try (IndexReader reader = DirectoryReader.open(indexWriter)) {
            LeafReaderContext ctx = reader.leaves().get(0);
            Number res = MaxAggregator.findLeafMaxValue(ctx.reader(), "number", pointConvertFunc);
            assertThat(res, equalTo(values.get(values.size() - 1).v2()));
        }
        for (int i = values.size() - 1; i > 0; i--) {
            indexWriter.deleteDocuments(new Term("id", values.get(i).v1().toString()));
            try (IndexReader reader = DirectoryReader.open(indexWriter)) {
                LeafReaderContext ctx = reader.leaves().get(0);
                Number res = MaxAggregator.findLeafMaxValue(ctx.reader(), "number", pointConvertFunc);
                if (res != null) {
                    assertThat(res, equalTo(values.get(i - 1).v2()));
                } else {
                    assertAllDeleted(ctx.reader().getLiveDocs(), ctx.reader().getPointValues("number"));
                }
            }
        }
        indexWriter.deleteDocuments(new Term("id", values.get(0).v1().toString()));
        try (IndexReader reader = DirectoryReader.open(indexWriter)) {
            LeafReaderContext ctx = reader.leaves().get(0);
            Number res = MaxAggregator.findLeafMaxValue(ctx.reader(), "number", pointConvertFunc);
            assertThat(res, equalTo(null));
        }
        indexWriter.close();
        directory.close();
    }

    // checks that documents inside the max leaves are all deleted
    private void assertAllDeleted(Bits liveDocs, PointValues values) throws IOException {
        final byte[] maxValue = values.getMaxPackedValue();
        int numBytes = values.getBytesPerDimension();
        final boolean[] seen = new boolean[1];
        values.intersect(new PointValues.IntersectVisitor() {
            @Override
            public void visit(int docID) {
                throw new AssertionError();
            }

            @Override
            public void visit(int docID, byte[] packedValue) {
                assertFalse(liveDocs.get(docID));
                seen[0] = true;
            }

            @Override
            public PointValues.Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
                if (Arrays.equals(maxPackedValue, 0, numBytes, maxValue, 0, numBytes)) {
                    return PointValues.Relation.CELL_CROSSES_QUERY;
                }
                return PointValues.Relation.CELL_OUTSIDE_QUERY;
            }
        });
        assertTrue(seen[0]);
    }

    public void testSingleValuedField() throws IOException {
        testAggregation(new MatchAllDocsQuery(), iw -> {
            final int numDocs = 10;
            for (int i = 0; i < numDocs; i++) {
                iw.addDocument(singleton(new NumericDocValuesField("number", i + 1)));
            }
        }, max -> {
            assertEquals(10, max.value(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(max));
        });
    }

    public void testSingleValuedFieldWithFormatter() throws IOException {
        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("value", NumberFieldMapper.NumberType.INTEGER);

        MaxAggregationBuilder aggregationBuilder = new MaxAggregationBuilder("_name").format("0000.0").field("value");

        testAggregation(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            final int numDocs = 10;
            for (int i = 0; i < numDocs; i++) {
                iw.addDocument(singleton(new NumericDocValuesField("value", i + 1)));
            }
        }, max -> {
            assertEquals(10.0, max.value(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(max));
            assertEquals("0010.0", max.getValueAsString());
        }, fieldType);
    }

    public void testSingleValuedFieldGetProperty() throws IOException {
        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("value", NumberFieldMapper.NumberType.INTEGER);

        AggregationBuilder aggregationBuilder = AggregationBuilders.global("global")
            .subAggregation(AggregationBuilders.max("max").field("value"));

        Directory directory = newDirectory();
        RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);
        final int numDocs = 10;
        for (int i = 0; i < numDocs; i++) {
            indexWriter.addDocument(singleton(new NumericDocValuesField("value", i + 1)));
        }
        indexWriter.close();

        DirectoryReader indexReader = DirectoryReader.open(directory);
        IndexSearcher indexSearcher = newIndexSearcher(indexReader);

        Global global = searchAndReduce(indexSearcher, new AggTestConfig(aggregationBuilder, fieldType));
        assertNotNull(global);
        assertEquals("global", global.getName());
        assertEquals(10L, global.getDocCount());
        assertNotNull(global.getAggregations());
        assertEquals(1, global.getAggregations().asMap().size());

        Max max = global.getAggregations().get("max");
        assertNotNull(max);
        assertEquals("max", max.getName());
        assertEquals(10.0, max.value(), 0);
        assertEquals(max, ((InternalAggregation) global).getProperty("max"));
        assertEquals(10.0, (double) ((InternalAggregation) global).getProperty("max.value"), 0);
        assertEquals(10.0, (double) ((InternalAggregation) max).getProperty("value"), 0);

        indexReader.close();
        directory.close();
    }

    public void testSingleValuedFieldWithValueScript() throws IOException {
        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("value", NumberFieldMapper.NumberType.INTEGER);

        MaxAggregationBuilder aggregationBuilder = new MaxAggregationBuilder("max").field("value")
            .script(new Script(ScriptType.INLINE, MockScriptEngine.NAME, VALUE_SCRIPT, Collections.emptyMap()));

        testAggregation(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            final int numDocs = 10;
            for (int i = 0; i < numDocs; i++) {
                iw.addDocument(singleton(new NumericDocValuesField("value", i + 1)));
            }
        }, max -> {
            assertTrue(AggregationInspectionHelper.hasValue(max));
            assertEquals(10.0, max.value(), 0);
            assertEquals("max", max.getName());
        }, fieldType);
    }

    public void testSingleValuedFieldWithValueScriptWithParams() throws IOException {
        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("value", NumberFieldMapper.NumberType.INTEGER);

        Map<String, Object> params = Collections.singletonMap("inc", 1);
        MaxAggregationBuilder aggregationBuilder = new MaxAggregationBuilder("max").field("value")
            .script(new Script(ScriptType.INLINE, MockScriptEngine.NAME, VALUE_SCRIPT, params));

        testAggregation(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            final int numDocs = 10;
            for (int i = 0; i < numDocs; i++) {
                iw.addDocument(singleton(new NumericDocValuesField("value", i + 1)));
            }
        }, max -> {
            assertEquals(11.0, max.value(), 0);
            assertEquals("max", max.getName());
            assertTrue(AggregationInspectionHelper.hasValue(max));
        }, fieldType);
    }

    public void testMultiValuedField() throws IOException {
        testAggregation(new MatchAllDocsQuery(), iw -> {
            final int numDocs = 10;
            for (int i = 0; i < numDocs; i++) {
                Document document = new Document();
                document.add(new SortedNumericDocValuesField("number", i + 2));
                document.add(new SortedNumericDocValuesField("number", i + 3));
                iw.addDocument(document);
            }
        }, max -> {
            assertEquals(12.0, max.value(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(max));
        });
    }

    public void testMultiValuedFieldWithValueScript() throws IOException {
        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("values", NumberFieldMapper.NumberType.INTEGER);

        MaxAggregationBuilder aggregationBuilder = new MaxAggregationBuilder("max").field("values")
            .script(new Script(ScriptType.INLINE, MockScriptEngine.NAME, VALUE_SCRIPT, Collections.emptyMap()));

        testAggregation(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            final int numDocs = 10;
            for (int i = 0; i < numDocs; i++) {
                Document document = new Document();
                document.add(new SortedNumericDocValuesField("values", i + 2));
                document.add(new SortedNumericDocValuesField("values", i + 3));
                iw.addDocument(document);
            }
        }, max -> {
            assertEquals(12.0, max.value(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(max));
        }, fieldType);
    }

    public void testMultiValuedFieldWithValueScriptWithParams() throws IOException {
        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("values", NumberFieldMapper.NumberType.INTEGER);

        Map<String, Object> params = Collections.singletonMap("inc", 1);
        MaxAggregationBuilder aggregationBuilder = new MaxAggregationBuilder("max").field("values")
            .script(new Script(ScriptType.INLINE, MockScriptEngine.NAME, VALUE_SCRIPT, params));

        testAggregation(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            final int numDocs = 10;
            for (int i = 0; i < numDocs; i++) {
                Document document = new Document();
                document.add(new SortedNumericDocValuesField("values", i + 2));
                document.add(new SortedNumericDocValuesField("values", i + 3));
                iw.addDocument(document);
            }
        }, max -> {
            assertEquals(13.0, max.value(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(max));
        }, fieldType);
    }

    public void testScriptSingleValued() throws IOException {
        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("value", NumberFieldMapper.NumberType.INTEGER);

        MaxAggregationBuilder aggregationBuilder = new MaxAggregationBuilder("max").script(
            new Script(ScriptType.INLINE, MockScriptEngine.NAME, VALUE_FIELD_SCRIPT, Collections.emptyMap())
        );

        testAggregation(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            final int numDocs = 10;
            for (int i = 0; i < numDocs; i++) {
                iw.addDocument(singleton(new NumericDocValuesField("value", i + 1)));
            }
        }, max -> {
            assertEquals(10.0, max.value(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(max));
        }, fieldType);
    }

    public void testScriptSingleValuedWithParams() throws IOException {
        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("value", NumberFieldMapper.NumberType.INTEGER);

        Map<String, Object> params = new HashMap<>();
        params.put("inc", 1);
        params.put("field", "value");

        MaxAggregationBuilder aggregationBuilder = new MaxAggregationBuilder("max").script(
            new Script(ScriptType.INLINE, MockScriptEngine.NAME, SUM_FIELD_PARAMS_SCRIPT, params)
        );

        testAggregation(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            final int numDocs = 10;
            for (int i = 0; i < numDocs; i++) {
                iw.addDocument(singleton(new NumericDocValuesField("value", i + 1)));
            }
        }, max -> {
            assertEquals(11.0, max.value(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(max));
        }, fieldType);
    }

    public void testScriptMultiValued() throws IOException {
        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("values", NumberFieldMapper.NumberType.INTEGER);

        MaxAggregationBuilder aggregationBuilder = new MaxAggregationBuilder("max").script(
            new Script(ScriptType.INLINE, MockScriptEngine.NAME, SUM_VALUES_FIELD_SCRIPT, Collections.emptyMap())
        );

        testAggregation(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            final int numDocs = 10;
            for (int i = 0; i < numDocs; i++) {
                Document document = new Document();
                document.add(new SortedNumericDocValuesField("values", i + 2));
                document.add(new SortedNumericDocValuesField("values", i + 3));
                iw.addDocument(document);
            }
        }, max -> {
            assertEquals(12.0, max.value(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(max));
        }, fieldType);
    }

    public void testScriptMultiValuedWithParams() throws IOException {
        Map<String, Object> params = new HashMap<>();
        params.put("inc", 1);
        params.put("field", "values");

        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("values", NumberFieldMapper.NumberType.INTEGER);

        MaxAggregationBuilder aggregationBuilder = new MaxAggregationBuilder("max").script(
            new Script(ScriptType.INLINE, MockScriptEngine.NAME, SUM_FIELD_PARAMS_SCRIPT, params)
        );

        testAggregation(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            final int numDocs = 10;
            for (int i = 0; i < numDocs; i++) {
                Document document = new Document();
                document.add(new SortedNumericDocValuesField("values", i + 2));
                document.add(new SortedNumericDocValuesField("values", i + 3));
                iw.addDocument(document);
            }
        }, max -> {
            assertEquals(13.0, max.value(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(max));
        }, fieldType);
    }

    public void testEmptyAggregation() throws Exception {
        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("value", NumberFieldMapper.NumberType.INTEGER);

        AggregationBuilder aggregationBuilder = AggregationBuilders.global("global")
            .subAggregation(AggregationBuilders.max("max").field("value"));

        Directory directory = newDirectory();
        RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);
        // Do not add any documents
        indexWriter.close();

        DirectoryReader indexReader = DirectoryReader.open(directory);
        IndexSearcher indexSearcher = newIndexSearcher(indexReader);

        Global global = searchAndReduce(indexSearcher, new AggTestConfig(aggregationBuilder, fieldType));
        assertNotNull(global);
        assertEquals("global", global.getName());
        assertEquals(0L, global.getDocCount());
        assertNotNull(global.getAggregations());
        assertEquals(1, global.getAggregations().asMap().size());

        Max max = global.getAggregations().get("max");
        assertNotNull(max);
        assertEquals("max", max.getName());
        assertEquals(Double.NEGATIVE_INFINITY, max.value(), 0);

        indexReader.close();
        directory.close();
    }

    public void testOrderByEmptyAggregation() throws IOException {
        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("value", NumberFieldMapper.NumberType.INTEGER);

        TermsAggregationBuilder aggregationBuilder = new TermsAggregationBuilder("terms").userValueTypeHint(ValueType.NUMERIC)
            .field("value")
            .order(BucketOrder.compound(BucketOrder.aggregation("filter>max", true)))
            .subAggregation(
                AggregationBuilders.filter("filter", termQuery("value", 100)).subAggregation(AggregationBuilders.max("max").field("value"))
            );

        Directory directory = newDirectory();
        RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);
        final int numDocs = 10;
        for (int i = 0; i < numDocs; i++) {
            indexWriter.addDocument(singleton(new NumericDocValuesField("value", i + 1)));
        }
        indexWriter.close();

        DirectoryReader indexReader = DirectoryReader.open(directory);
        IndexSearcher indexSearcher = newIndexSearcher(indexReader);

        Terms terms = searchAndReduce(indexSearcher, new AggTestConfig(aggregationBuilder, fieldType));
        assertNotNull(terms);
        List<? extends Terms.Bucket> buckets = terms.getBuckets();
        assertNotNull(buckets);
        assertEquals(10, buckets.size());

        for (int i = 0; i < 10; i++) {
            Terms.Bucket bucket = buckets.get(i);
            assertNotNull(bucket);
            assertEquals((long) i + 1, bucket.getKeyAsNumber());
            assertEquals(1L, bucket.getDocCount());

            Filter filter = bucket.getAggregations().get("filter");
            assertNotNull(filter);
            assertEquals(0L, filter.getDocCount());

            Max max = filter.getAggregations().get("max");
            assertNotNull(max);
            assertEquals(Double.NEGATIVE_INFINITY, max.value(), 0);
        }

        indexReader.close();
        directory.close();
    }

    public void testEarlyTermination() throws Exception {
        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("values", NumberFieldMapper.NumberType.INTEGER);

        Directory directory = newDirectory();
        RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);
        final int numDocs = 10;
        for (int i = 0; i < numDocs; i++) {
            Document document = new Document();
            document.add(new SortedNumericDocValuesField("values", i + 2));
            document.add(new SortedNumericDocValuesField("values", i + 3));
            indexWriter.addDocument(document);
        }
        indexWriter.close();

        DirectoryReader indexReader = DirectoryReader.open(directory);
        IndexSearcher indexSearcher = newIndexSearcher(indexReader);

        MaxAggregationBuilder maxAggregationBuilder = new MaxAggregationBuilder("max").field("values");
        ValueCountAggregationBuilder countAggregationBuilder = new ValueCountAggregationBuilder("count").field("values");

        try (AggregationContext context = createAggregationContext(indexSearcher, new MatchAllDocsQuery(), fieldType)) {
            MaxAggregator maxAggregator = createAggregator(maxAggregationBuilder, context);
            ValueCountAggregator countAggregator = createAggregator(countAggregationBuilder, context);

            BucketCollector bucketCollector = MultiBucketCollector.wrap(true, List.of(maxAggregator, countAggregator));
            bucketCollector.preCollection();
            indexSearcher.search(new MatchAllDocsQuery(), bucketCollector.asCollector());
            bucketCollector.postCollection();

            Max max = (Max) maxAggregator.buildAggregation(0L);
            assertNotNull(max);
            assertEquals(12.0, max.value(), 0);
            assertEquals("max", max.getName());

            InternalValueCount count = (InternalValueCount) countAggregator.buildAggregation(0L);
            assertNotNull(count);
            assertEquals(20L, count.getValue());
            assertEquals("count", count.getName());

        }
        indexReader.close();
        directory.close();
    }

    public void testNestedEarlyTermination() throws Exception {
        MappedFieldType multiValuesfieldType = new NumberFieldMapper.NumberFieldType("values", NumberFieldMapper.NumberType.INTEGER);
        MappedFieldType singleValueFieldType = new NumberFieldMapper.NumberFieldType("value", NumberFieldMapper.NumberType.INTEGER);

        Directory directory = newDirectory();
        RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);
        final int numDocs = 10;
        for (int i = 0; i < numDocs; i++) {
            Document document = new Document();
            document.add(new NumericDocValuesField("value", i + 1));
            document.add(new SortedNumericDocValuesField("values", i + 2));
            document.add(new SortedNumericDocValuesField("values", i + 3));
            indexWriter.addDocument(document);
        }
        indexWriter.close();

        DirectoryReader indexReader = DirectoryReader.open(directory);
        IndexSearcher indexSearcher = newIndexSearcher(indexReader);

        try (
            AggregationContext context = createAggregationContext(
                indexSearcher,
                new MatchAllDocsQuery(),
                multiValuesfieldType,
                singleValueFieldType
            )
        ) {
            for (Aggregator.SubAggCollectionMode collectionMode : Aggregator.SubAggCollectionMode.values()) {
                MaxAggregationBuilder maxAggregationBuilder = new MaxAggregationBuilder("max").field("values");
                ValueCountAggregationBuilder countAggregationBuilder = new ValueCountAggregationBuilder("count").field("values");
                TermsAggregationBuilder termsAggregationBuilder = new TermsAggregationBuilder("terms").userValueTypeHint(ValueType.NUMERIC)
                    .field("value")
                    .collectMode(collectionMode)
                    .subAggregation(new MaxAggregationBuilder("sub_max").field("invalid"));

                MaxAggregator maxAggregator = createAggregator(maxAggregationBuilder, context);
                ValueCountAggregator countAggregator = createAggregator(countAggregationBuilder, context);
                TermsAggregator termsAggregator = createAggregator(termsAggregationBuilder, context);

                BucketCollector bucketCollector = MultiBucketCollector.wrap(true, List.of(maxAggregator, countAggregator, termsAggregator));
                bucketCollector.preCollection();
                indexSearcher.search(new MatchAllDocsQuery(), bucketCollector.asCollector());
                bucketCollector.postCollection();

                Max max = (Max) maxAggregator.buildTopLevel();
                assertNotNull(max);
                assertEquals(12.0, max.value(), 0);
                assertEquals("max", max.getName());

                InternalValueCount count = (InternalValueCount) countAggregator.buildTopLevel();
                assertNotNull(count);
                assertEquals(20L, count.getValue());
                assertEquals("count", count.getName());

                Terms terms = (Terms) termsAggregator.buildTopLevel();
                assertNotNull(terms);
                List<? extends Terms.Bucket> buckets = terms.getBuckets();
                assertNotNull(buckets);
                assertEquals(10, buckets.size());

                for (Terms.Bucket b : buckets) {
                    Max subMax = b.getAggregations().get("sub_max");
                    assertEquals(Double.NEGATIVE_INFINITY, subMax.value(), 0);
                }
            }
        }

        indexReader.close();
        directory.close();
    }

    /**
     * Make sure that an aggregation not using a script does get cached.
     */
    public void testCacheAggregation() throws IOException {
        Directory directory = newDirectory();
        RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);
        final int numDocs = 10;
        for (int i = 0; i < numDocs; i++) {
            indexWriter.addDocument(singleton(new NumericDocValuesField("value", i + 1)));
        }
        indexWriter.addDocument(singleton(new NumericDocValuesField("unrelated", 100)));
        indexWriter.close();

        DirectoryReader indexReader = DirectoryReader.open(directory);
        IndexSearcher indexSearcher = newIndexSearcher(indexReader);

        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("value", NumberFieldMapper.NumberType.INTEGER);
        MaxAggregationBuilder aggregationBuilder = new MaxAggregationBuilder("max").field("value");

        Max max = searchAndReduce(indexSearcher, new AggTestConfig(aggregationBuilder, fieldType));

        assertEquals(10.0, max.value(), 0);
        assertEquals("max", max.getName());
        assertTrue(AggregationInspectionHelper.hasValue(max));

        indexReader.close();
        directory.close();
    }

    /**
     * Make sure that a request using a deterministic script or not using a script get cached.
     * Ensure requests using nondeterministic scripts do not get cached.
     */
    public void testScriptCaching() throws Exception {
        Directory directory = newDirectory();
        RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);
        final int numDocs = 10;
        for (int i = 0; i < numDocs; i++) {
            indexWriter.addDocument(singleton(new NumericDocValuesField("value", i + 1)));
        }
        indexWriter.addDocument(singleton(new NumericDocValuesField("unrelated", 100)));
        indexWriter.close();

        DirectoryReader indexReader = DirectoryReader.open(directory);
        IndexSearcher indexSearcher = newIndexSearcher(indexReader);

        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("value", NumberFieldMapper.NumberType.INTEGER);
        MaxAggregationBuilder aggregationBuilder = new MaxAggregationBuilder("max").field("value")
            .script(new Script(ScriptType.INLINE, MockScriptEngine.NAME, VALUE_SCRIPT, Collections.emptyMap()));

        Max max = searchAndReduce(indexSearcher, new AggTestConfig(aggregationBuilder, fieldType));

        assertEquals(10.0, max.value(), 0);
        assertEquals("max", max.getName());
        assertTrue(AggregationInspectionHelper.hasValue(max));

        aggregationBuilder = new MaxAggregationBuilder("max").field("value")
            .script(new Script(ScriptType.INLINE, MockScriptEngine.NAME, RANDOM_SCRIPT, Collections.emptyMap()));
        max = searchAndReduce(indexSearcher, new AggTestConfig(aggregationBuilder, fieldType).withShouldBeCached(false));

        assertTrue(max.value() >= 0.0);
        assertTrue(max.value() <= 1.0);
        assertEquals("max", max.getName());
        assertTrue(AggregationInspectionHelper.hasValue(max));

        indexReader.close();
        directory.close();
    }
}
