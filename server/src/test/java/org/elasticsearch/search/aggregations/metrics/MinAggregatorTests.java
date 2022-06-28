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
import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.mapper.IpFieldMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.script.MockScriptEngine;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptEngine;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.filter.Filter;
import org.elasticsearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.global.GlobalAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.global.InternalGlobal;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalHistogram;
import org.elasticsearch.search.aggregations.bucket.terms.InternalTerms;
import org.elasticsearch.search.aggregations.bucket.terms.LongTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.AggregationInspectionHelper;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
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

import static java.util.Collections.singleton;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.hamcrest.Matchers.equalTo;

public class MinAggregatorTests extends AggregatorTestCase {

    private static final String SCRIPT_NAME = "script_name";
    private static final long SCRIPT_VALUE = 19L;

    /** Script to take a field name in params and sum the values of the field. */
    private static final String SUM_FIELD_PARAMS_SCRIPT = "sum_field_params";

    /** Script to sum the values of a field named {@code values}. */
    private static final String SUM_VALUES_FIELD_SCRIPT = "sum_values_field";

    /** Script to return the value of a field named {@code value}. */
    private static final String VALUE_FIELD_SCRIPT = "value_field";

    /** Script to return the {@code _value} provided by aggs framework. */
    private static final String VALUE_SCRIPT = "_value";

    private static final String INVERT_SCRIPT = "invert";

    private static final String RANDOM_SCRIPT = "random";

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
        scripts.put(INVERT_SCRIPT, vars -> -((Number) vars.get("_value")).doubleValue());

        Map<String, Function<Map<String, Object>, Object>> nonDeterministicScripts = new HashMap<>();
        nonDeterministicScripts.put(RANDOM_SCRIPT, vars -> AggregatorTestCase.randomDouble());

        MockScriptEngine scriptEngine = new MockScriptEngine(
            MockScriptEngine.NAME,
            scripts,
            nonDeterministicScripts,
            Collections.emptyMap()
        );
        Map<String, ScriptEngine> engines = Collections.singletonMap(scriptEngine.getType(), scriptEngine);

        return new ScriptService(Settings.EMPTY, engines, ScriptModule.CORE_CONTEXTS);
    }

    public void testNoMatchingField() throws IOException {
        testCase(new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new SortedNumericDocValuesField("wrong_number", 7)));
            iw.addDocument(singleton(new SortedNumericDocValuesField("wrong_number", 3)));
        }, min -> {
            assertEquals(Double.POSITIVE_INFINITY, min.getValue(), 0);
            assertFalse(AggregationInspectionHelper.hasValue(min));
        });
    }

    public void testMatchesSortedNumericDocValues() throws IOException {
        testCase(new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new SortedNumericDocValuesField("number", 7)));
            iw.addDocument(singleton(new SortedNumericDocValuesField("number", 2)));
            iw.addDocument(singleton(new SortedNumericDocValuesField("number", 3)));
        }, min -> {
            assertEquals(2, min.getValue(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(min));
        });
    }

    public void testMatchesNumericDocValues() throws IOException {
        testCase(new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new NumericDocValuesField("number", 7)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 2)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 3)));
        }, min -> {
            assertEquals(2, min.getValue(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(min));
        });
    }

    public void testSomeMatchesSortedNumericDocValues() throws IOException {
        testCase(new DocValuesFieldExistsQuery("number"), iw -> {
            iw.addDocument(singleton(new SortedNumericDocValuesField("number", 7)));
            iw.addDocument(singleton(new SortedNumericDocValuesField("number2", 2)));
            iw.addDocument(singleton(new SortedNumericDocValuesField("number", 3)));
        }, min -> {
            assertEquals(3, min.getValue(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(min));
        });
    }

    public void testSomeMatchesNumericDocValues() throws IOException {
        testCase(new DocValuesFieldExistsQuery("number"), iw -> {
            iw.addDocument(singleton(new NumericDocValuesField("number", 7)));
            iw.addDocument(singleton(new NumericDocValuesField("number2", 2)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 3)));
        }, min -> {
            assertEquals(3, min.getValue(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(min));
        });
    }

    public void testQueryFiltering() throws IOException {
        testCase(IntPoint.newRangeQuery("number", 0, 3), iw -> {
            iw.addDocument(Arrays.asList(new IntPoint("number", 7), new SortedNumericDocValuesField("number", 7)));
            iw.addDocument(Arrays.asList(new IntPoint("number", 1), new SortedNumericDocValuesField("number", 1)));
            iw.addDocument(Arrays.asList(new IntPoint("number", 3), new SortedNumericDocValuesField("number", 3)));
        }, min -> {
            assertEquals(1, min.getValue(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(min));
        });
    }

    public void testQueryFiltersAll() throws IOException {
        testCase(IntPoint.newRangeQuery("number", -1, 0), iw -> {
            iw.addDocument(Arrays.asList(new IntPoint("number", 7), new SortedNumericDocValuesField("number", 7)));
            iw.addDocument(Arrays.asList(new IntPoint("number", 1), new SortedNumericDocValuesField("number", 1)));
            iw.addDocument(Arrays.asList(new IntPoint("number", 3), new SortedNumericDocValuesField("number", 3)));
        }, min -> {
            assertEquals(Double.POSITIVE_INFINITY, min.getValue(), 0);
            assertFalse(AggregationInspectionHelper.hasValue(min));
        });
    }

    public void testIpField() throws IOException {
        final String fieldName = "IP_field";
        MinAggregationBuilder aggregationBuilder = new MinAggregationBuilder("min").field(fieldName);

        MappedFieldType fieldType = new IpFieldMapper.IpFieldType(fieldName);
        boolean v4 = randomBoolean();
        expectThrows(IllegalArgumentException.class, () -> testCase(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new SortedSetDocValuesField(fieldName, new BytesRef(InetAddressPoint.encode(randomIp(v4))))));
            iw.addDocument(singleton(new SortedSetDocValuesField(fieldName, new BytesRef(InetAddressPoint.encode(randomIp(v4))))));
        }, min -> fail("expected an exception"), fieldType));
    }

    public void testUnmappedWithMissingField() throws IOException {
        MinAggregationBuilder aggregationBuilder = new MinAggregationBuilder("min").field("does_not_exist").missing(0L);

        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("number", NumberFieldMapper.NumberType.INTEGER);

        testCase(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new NumericDocValuesField("number", 7)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 1)));
        }, (Consumer<InternalMin>) min -> {
            assertEquals(0.0, min.getValue(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(min));
        }, fieldType);
    }

    public void testUnsupportedType() {
        MinAggregationBuilder aggregationBuilder = new MinAggregationBuilder("min").field("not_a_number");

        MappedFieldType fieldType = new KeywordFieldMapper.KeywordFieldType("not_a_number");

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> testCase(
                aggregationBuilder,
                new MatchAllDocsQuery(),
                iw -> { iw.addDocument(singleton(new SortedSetDocValuesField("string", new BytesRef("foo")))); },
                (Consumer<InternalMin>) min -> { fail("Should have thrown exception"); },
                fieldType
            )
        );
        assertEquals("Field [not_a_number] of type [keyword] is not supported for aggregation [min]", e.getMessage());
    }

    public void testBadMissingField() {
        MinAggregationBuilder aggregationBuilder = new MinAggregationBuilder("min").field("number").missing("not_a_number");

        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("number", NumberFieldMapper.NumberType.INTEGER);

        expectThrows(NumberFormatException.class, () -> testCase(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new NumericDocValuesField("number", 7)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 1)));
        }, (Consumer<InternalMin>) min -> { fail("Should have thrown exception"); }, fieldType));
    }

    public void testUnmappedWithBadMissingField() {
        MinAggregationBuilder aggregationBuilder = new MinAggregationBuilder("min").field("does_not_exist").missing("not_a_number");

        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("number", NumberFieldMapper.NumberType.INTEGER);

        expectThrows(NumberFormatException.class, () -> testCase(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new NumericDocValuesField("number", 7)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 1)));
        }, (Consumer<InternalMin>) min -> { fail("Should have thrown exception"); }, fieldType));
    }

    public void testEmptyBucket() throws IOException {
        HistogramAggregationBuilder histogram = new HistogramAggregationBuilder("histo").field("number")
            .interval(1)
            .minDocCount(0)
            .subAggregation(new MinAggregationBuilder("min").field("number"));

        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("number", NumberFieldMapper.NumberType.INTEGER);

        testCase(histogram, new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new NumericDocValuesField("number", 1)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 3)));
        }, (Consumer<InternalHistogram>) histo -> {
            assertThat(histo.getBuckets().size(), equalTo(3));

            assertNotNull(histo.getBuckets().get(0).getAggregations().asMap().get("min"));
            InternalMin min = (InternalMin) histo.getBuckets().get(0).getAggregations().asMap().get("min");
            assertEquals(1.0, min.getValue(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(min));

            assertNotNull(histo.getBuckets().get(1).getAggregations().asMap().get("min"));
            min = (InternalMin) histo.getBuckets().get(1).getAggregations().asMap().get("min");
            assertEquals(Double.POSITIVE_INFINITY, min.getValue(), 0);
            assertFalse(AggregationInspectionHelper.hasValue(min));

            assertNotNull(histo.getBuckets().get(2).getAggregations().asMap().get("min"));
            min = (InternalMin) histo.getBuckets().get(2).getAggregations().asMap().get("min");
            assertEquals(3.0, min.getValue(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(min));

        }, fieldType);
    }

    public void testFormatter() throws IOException {
        MinAggregationBuilder aggregationBuilder = new MinAggregationBuilder("min").field("number").format("0000.0");

        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("number", NumberFieldMapper.NumberType.INTEGER);

        testCase(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new NumericDocValuesField("number", 7)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 1)));
        }, (Consumer<InternalMin>) min -> {
            assertEquals(1.0, min.getValue(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(min));
            assertEquals("0001.0", min.getValueAsString());
        }, fieldType);
    }

    public void testGetProperty() throws IOException {
        GlobalAggregationBuilder globalBuilder = new GlobalAggregationBuilder("global").subAggregation(
            new MinAggregationBuilder("min").field("number")
        );

        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("number", NumberFieldMapper.NumberType.INTEGER);

        testCase(globalBuilder, new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new NumericDocValuesField("number", 7)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 1)));
        }, (Consumer<InternalGlobal>) global -> {
            assertEquals(2, global.getDocCount());
            assertTrue(AggregationInspectionHelper.hasValue(global));
            assertNotNull(global.getAggregations().asMap().get("min"));

            InternalMin min = (InternalMin) global.getAggregations().asMap().get("min");
            assertEquals(1.0, min.getValue(), 0);
            assertThat(global.getProperty("min"), equalTo(min));
            assertThat(global.getProperty("min.value"), equalTo(1.0));
            assertThat(min.getProperty("value"), equalTo(1.0));
        }, fieldType);
    }

    public void testSingleValuedFieldPartiallyUnmapped() throws IOException {

        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("number", NumberFieldMapper.NumberType.INTEGER);
        MinAggregationBuilder aggregationBuilder = new MinAggregationBuilder("min").field("number");

        try (Directory directory = newDirectory(); Directory unmappedDirectory = newDirectory()) {
            RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);
            indexWriter.addDocument(singleton(new NumericDocValuesField("number", 7)));
            indexWriter.addDocument(singleton(new NumericDocValuesField("number", 2)));
            indexWriter.addDocument(singleton(new NumericDocValuesField("number", 3)));
            indexWriter.close();

            RandomIndexWriter unmappedIndexWriter = new RandomIndexWriter(random(), unmappedDirectory);
            unmappedIndexWriter.close();

            try (
                IndexReader indexReader = DirectoryReader.open(directory);
                IndexReader unamappedIndexReader = DirectoryReader.open(unmappedDirectory)
            ) {

                MultiReader multiReader = new MultiReader(indexReader, unamappedIndexReader);
                IndexSearcher indexSearcher = newSearcher(multiReader, true, true);

                InternalMin min = searchAndReduce(indexSearcher, new MatchAllDocsQuery(), aggregationBuilder, fieldType);
                assertEquals(2.0, min.getValue(), 0);
                assertTrue(AggregationInspectionHelper.hasValue(min));
            }
        }
    }

    public void testSingleValuedFieldPartiallyUnmappedWithMissing() throws IOException {

        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("number", NumberFieldMapper.NumberType.INTEGER);
        MinAggregationBuilder aggregationBuilder = new MinAggregationBuilder("min").field("number").missing(-19L);

        try (Directory directory = newDirectory(); Directory unmappedDirectory = newDirectory()) {
            RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);
            indexWriter.addDocument(singleton(new NumericDocValuesField("number", 7)));
            indexWriter.addDocument(singleton(new NumericDocValuesField("number", 2)));
            indexWriter.addDocument(singleton(new NumericDocValuesField("number", 3)));
            indexWriter.close();

            RandomIndexWriter unmappedIndexWriter = new RandomIndexWriter(random(), unmappedDirectory);
            unmappedIndexWriter.addDocument(singleton(new NumericDocValuesField("unrelated", 100)));
            unmappedIndexWriter.close();

            try (
                IndexReader indexReader = DirectoryReader.open(directory);
                IndexReader unamappedIndexReader = DirectoryReader.open(unmappedDirectory)
            ) {

                MultiReader multiReader = new MultiReader(indexReader, unamappedIndexReader);
                IndexSearcher indexSearcher = newSearcher(multiReader, true, true);

                InternalMin min = searchAndReduce(indexSearcher, new MatchAllDocsQuery(), aggregationBuilder, fieldType);
                assertEquals(-19.0, min.getValue(), 0);
                assertTrue(AggregationInspectionHelper.hasValue(min));
            }
        }
    }

    public void testSingleValuedFieldWithValueScript() throws IOException {
        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("number", NumberFieldMapper.NumberType.INTEGER);

        MinAggregationBuilder aggregationBuilder = new MinAggregationBuilder("min").field("number")
            .script(new Script(ScriptType.INLINE, MockScriptEngine.NAME, INVERT_SCRIPT, Collections.emptyMap()));

        testCase(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            final int numDocs = 10;
            for (int i = 0; i < numDocs; i++) {
                iw.addDocument(singleton(new NumericDocValuesField("number", i + 1)));
            }
        }, (Consumer<InternalMin>) min -> {
            assertEquals(-10.0, min.getValue(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(min));
        }, fieldType);
    }

    public void testSingleValuedFieldWithValueScriptAndMissing() throws IOException {
        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("number", NumberFieldMapper.NumberType.INTEGER);

        MinAggregationBuilder aggregationBuilder = new MinAggregationBuilder("min").field("number")
            .missing(-100L)
            .script(new Script(ScriptType.INLINE, MockScriptEngine.NAME, INVERT_SCRIPT, Collections.emptyMap()));

        testCase(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            final int numDocs = 10;
            for (int i = 0; i < numDocs; i++) {
                iw.addDocument(singleton(new NumericDocValuesField("number", i + 1)));
            }
            iw.addDocument(singleton(new NumericDocValuesField("unrelated", 1)));
        }, (Consumer<InternalMin>) min -> {
            assertEquals(-100.0, min.getValue(), 0); // Note: this comes straight from missing, and is not inverted from script
            assertTrue(AggregationInspectionHelper.hasValue(min));
        }, fieldType);
    }

    public void testSingleValuedFieldWithValueScriptAndParams() throws IOException {
        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("number", NumberFieldMapper.NumberType.INTEGER);

        MinAggregationBuilder aggregationBuilder = new MinAggregationBuilder("min").field("number")
            .script(new Script(ScriptType.INLINE, MockScriptEngine.NAME, VALUE_SCRIPT, Collections.singletonMap("inc", 5)));

        testCase(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            final int numDocs = 10;
            for (int i = 0; i < numDocs; i++) {
                iw.addDocument(singleton(new NumericDocValuesField("number", i + 1)));
            }
        }, (Consumer<InternalMin>) min -> {
            assertEquals(6.0, min.getValue(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(min));
        }, fieldType);
    }

    public void testScript() throws IOException {
        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("number", NumberFieldMapper.NumberType.INTEGER);

        MinAggregationBuilder aggregationBuilder = new MinAggregationBuilder("min").script(
            new Script(ScriptType.INLINE, MockScriptEngine.NAME, SCRIPT_NAME, Collections.emptyMap())
        );

        testCase(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            final int numDocs = 10;
            for (int i = 0; i < numDocs; i++) {
                iw.addDocument(singleton(new NumericDocValuesField("number", i + 1)));
            }
        }, (Consumer<InternalMin>) min -> {
            assertEquals(19.0, min.getValue(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(min));
        }, fieldType);
    }

    public void testMultiValuedField() throws IOException {
        MinAggregationBuilder aggregationBuilder = new MinAggregationBuilder("min").field("number");

        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("number", NumberFieldMapper.NumberType.INTEGER);

        testCase(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            final int numDocs = 10;
            for (int i = 0; i < numDocs; i++) {
                Document document = new Document();
                document.add(new SortedNumericDocValuesField("number", i + 2));
                document.add(new SortedNumericDocValuesField("number", i + 3));
                iw.addDocument(document);
            }
        }, (Consumer<InternalMin>) min -> {
            assertEquals(2.0, min.getValue(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(min));
        }, fieldType);
    }

    public void testMultiValuedFieldWithScript() throws IOException {
        MinAggregationBuilder aggregationBuilder = new MinAggregationBuilder("min").field("number")
            .script(new Script(ScriptType.INLINE, MockScriptEngine.NAME, INVERT_SCRIPT, Collections.emptyMap()));

        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("number", NumberFieldMapper.NumberType.INTEGER);

        testCase(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            final int numDocs = 10;
            for (int i = 0; i < numDocs; i++) {
                Document document = new Document();
                document.add(new SortedNumericDocValuesField("number", i + 2));
                document.add(new SortedNumericDocValuesField("number", i + 3));
                iw.addDocument(document);
            }
        }, (Consumer<InternalMin>) min -> {
            assertEquals(-12.0, min.getValue(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(min));
        }, fieldType);
    }

    public void testMultiValuedFieldWithScriptParams() throws IOException {
        MinAggregationBuilder aggregationBuilder = new MinAggregationBuilder("min").field("number")
            .script(new Script(ScriptType.INLINE, MockScriptEngine.NAME, VALUE_SCRIPT, Collections.singletonMap("inc", 5)));

        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("number", NumberFieldMapper.NumberType.INTEGER);

        testCase(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            final int numDocs = 10;
            for (int i = 0; i < numDocs; i++) {
                Document document = new Document();
                document.add(new SortedNumericDocValuesField("number", i + 2));
                document.add(new SortedNumericDocValuesField("number", i + 3));
                iw.addDocument(document);
            }
        }, (Consumer<InternalMin>) min -> {
            assertEquals(7.0, min.getValue(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(min));
        }, fieldType);
    }

    public void testOrderByEmptyAggregation() throws IOException {
        AggregationBuilder termsBuilder = new TermsAggregationBuilder("terms").field("number")
            .order(BucketOrder.compound(BucketOrder.aggregation("filter>min", true)))
            .subAggregation(
                new FilterAggregationBuilder("filter", termQuery("number", 100)).subAggregation(
                    new MinAggregationBuilder("min").field("number")
                )
            );

        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("number", NumberFieldMapper.NumberType.INTEGER);

        int numDocs = 10;
        testCase(termsBuilder, new MatchAllDocsQuery(), iw -> {
            for (int i = 0; i < numDocs; i++) {
                iw.addDocument(singleton(new NumericDocValuesField("number", i + 1)));
            }
        }, (Consumer<InternalTerms<?, LongTerms.Bucket>>) terms -> {
            for (int i = 0; i < numDocs; i++) {
                List<LongTerms.Bucket> buckets = terms.getBuckets();
                Terms.Bucket bucket = buckets.get(i);
                assertNotNull(bucket);
                assertEquals((long) i + 1, bucket.getKeyAsNumber());
                assertEquals(1L, bucket.getDocCount());

                Filter filter = bucket.getAggregations().get("filter");
                assertNotNull(filter);
                assertEquals(0L, filter.getDocCount());

                InternalMin min = filter.getAggregations().get("min");
                assertNotNull(min);
                assertEquals(Double.POSITIVE_INFINITY, min.getValue(), 0);
                assertFalse(AggregationInspectionHelper.hasValue(min));
            }
        }, fieldType);
    }

    public void testCaching() throws IOException {

        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("number", NumberFieldMapper.NumberType.INTEGER);
        MinAggregationBuilder aggregationBuilder = new MinAggregationBuilder("min").field("number");

        try (Directory directory = newDirectory()) {
            RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);
            indexWriter.addDocument(singleton(new NumericDocValuesField("number", 7)));
            indexWriter.addDocument(singleton(new NumericDocValuesField("number", 2)));
            indexWriter.addDocument(singleton(new NumericDocValuesField("number", 3)));
            indexWriter.close();

            try (IndexReader indexReader = DirectoryReader.open(directory)) {
                IndexSearcher indexSearcher = newSearcher(indexReader, true, true);

                AggregationContext context = createAggregationContext(indexSearcher, new MatchAllDocsQuery(), fieldType);
                createAggregator(aggregationBuilder, context);
                assertTrue(context.isCacheable());
            }
        }
    }

    public void testScriptCaching() throws IOException {

        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("number", NumberFieldMapper.NumberType.INTEGER);
        MinAggregationBuilder aggregationBuilder = new MinAggregationBuilder("min").field("number")
            .script(new Script(ScriptType.INLINE, MockScriptEngine.NAME, INVERT_SCRIPT, Collections.emptyMap()));

        MinAggregationBuilder nonDeterministicAggregationBuilder = new MinAggregationBuilder("min").field("number")
            .script(new Script(ScriptType.INLINE, MockScriptEngine.NAME, RANDOM_SCRIPT, Collections.emptyMap()));

        try (Directory directory = newDirectory()) {
            RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);
            indexWriter.addDocument(singleton(new NumericDocValuesField("number", 7)));
            indexWriter.addDocument(singleton(new NumericDocValuesField("number", 2)));
            indexWriter.addDocument(singleton(new NumericDocValuesField("number", 3)));
            indexWriter.close();

            try (IndexReader indexReader = DirectoryReader.open(directory)) {
                IndexSearcher indexSearcher = newSearcher(indexReader, true, true);

                AggregationContext context = createAggregationContext(indexSearcher, new MatchAllDocsQuery(), fieldType);
                createAggregator(nonDeterministicAggregationBuilder, context);
                assertFalse(context.isCacheable());

                context = createAggregationContext(indexSearcher, new MatchAllDocsQuery(), fieldType);
                createAggregator(aggregationBuilder, context);
                assertTrue(context.isCacheable());
            }
        }
    }

    public void testMinShortcutRandom() throws Exception {
        testMinShortcutCase(
            () -> randomLongBetween(Integer.MIN_VALUE, Integer.MAX_VALUE),
            (n) -> new LongPoint("number", n.longValue()),
            (v) -> LongPoint.decodeDimension(v, 0)
        );

        testMinShortcutCase(() -> randomInt(), (n) -> new IntPoint("number", n.intValue()), (v) -> IntPoint.decodeDimension(v, 0));

        testMinShortcutCase(() -> randomFloat(), (n) -> new FloatPoint("number", n.floatValue()), (v) -> FloatPoint.decodeDimension(v, 0));

        testMinShortcutCase(
            () -> randomDouble(),
            (n) -> new DoublePoint("number", n.doubleValue()),
            (v) -> DoublePoint.decodeDimension(v, 0)
        );
    }

    private void testMinShortcutCase(
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
            Number res = MinAggregator.findLeafMinValue(ctx.reader(), "number", pointConvertFunc);
            assertThat(res, equalTo(values.get(0).v2()));
        }
        for (int i = 1; i < values.size(); i++) {
            indexWriter.deleteDocuments(new Term("id", values.get(i - 1).v1().toString()));
            try (IndexReader reader = DirectoryReader.open(indexWriter)) {
                LeafReaderContext ctx = reader.leaves().get(0);
                Number res = MinAggregator.findLeafMinValue(ctx.reader(), "number", pointConvertFunc);
                assertThat(res, equalTo(values.get(i).v2()));
            }
        }
        indexWriter.deleteDocuments(new Term("id", values.get(values.size() - 1).v1().toString()));
        try (IndexReader reader = DirectoryReader.open(indexWriter)) {
            LeafReaderContext ctx = reader.leaves().get(0);
            Number res = MinAggregator.findLeafMinValue(ctx.reader(), "number", pointConvertFunc);
            assertThat(res, equalTo(null));
        }
        indexWriter.close();
        directory.close();
    }

    private void testCase(Query query, CheckedConsumer<RandomIndexWriter, IOException> buildIndex, Consumer<InternalMin> verify)
        throws IOException {
        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("number", NumberFieldMapper.NumberType.INTEGER);
        MinAggregationBuilder aggregationBuilder = new MinAggregationBuilder("min").field("number");
        testCase(aggregationBuilder, query, buildIndex, verify, fieldType);
    }

    @Override
    protected List<ValuesSourceType> getSupportedValuesSourceTypes() {
        return Arrays.asList(CoreValuesSourceType.NUMERIC, CoreValuesSourceType.DATE, CoreValuesSourceType.BOOLEAN);
    }

    @Override
    protected AggregationBuilder createAggBuilderForTypeTest(MappedFieldType fieldType, String fieldName) {
        return new MinAggregationBuilder("foo").field(fieldName);
    }
}
