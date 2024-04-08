/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.nested;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KeywordField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LogDocMergePolicy;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.LuceneDocument;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.index.mapper.NestedObjectMapper;
import org.elasticsearch.index.mapper.NestedPathFieldMapper;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.mapper.ObjectMapper;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.script.MockScriptEngine;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptEngine;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.bucket.filter.Filter;
import org.elasticsearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.InternalTerms;
import org.elasticsearch.search.aggregations.bucket.terms.LongTerms;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.Max;
import org.elasticsearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.Min;
import org.elasticsearch.search.aggregations.metrics.MinAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.Sum;
import org.elasticsearch.search.aggregations.metrics.SumAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.BucketScriptPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.InternalSimpleValue;
import org.elasticsearch.search.aggregations.support.AggregationInspectionHelper;
import org.elasticsearch.search.aggregations.support.ValueType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.DoubleStream;
import java.util.stream.LongStream;

import static java.util.stream.Collectors.toList;
import static org.elasticsearch.search.aggregations.AggregationBuilders.max;
import static org.elasticsearch.search.aggregations.AggregationBuilders.nested;
import static org.hamcrest.Matchers.equalTo;

/**
 * Tests for the Nested aggregator.
 *
 * <p>
 * Notes to people wanting to add nested aggregation tests to other test classes:
 * <ul>
 *     <li>Nested aggregations require a different {@link DirectoryReader} implementation than we usually use in aggregation tests.  You'll
 *     need to override {@link AggregatorTestCase#wrapDirectoryReader} as is done in this class</li>
 *     <li>Nested aggregations  also require object mappers to be configured.  You can mock this by overriding
 *     {@link AggregatorTestCase#objectMappers()} as seen below</li>
 *     <li>In a production nested field setup, we'll automatically prefix the nested path to the leaf document field names.  This helps
 *     prevent name collisions between "levels" of nested docs.  This mechanism isn't invoked during unit tests, so preventing field name
 *     collisions should be done by hand. For the closest approximation of how it looks in prod, leaf docs should have field names
 *     prefixed with the nested path: nestedPath + "." + fieldName</li>
 * </ul>
 */
public class NestedAggregatorTests extends AggregatorTestCase {

    private static final String VALUE_FIELD_NAME = "number";
    private static final String NESTED_OBJECT = "nested_object";
    private static final String NESTED_OBJECT2 = "nested_object2";
    private static final String NESTED_AGG = "nestedAgg";
    private static final String MAX_AGG_NAME = "maxAgg";
    private static final String SUM_AGG_NAME = "sumAgg";
    private static final String INVERSE_SCRIPT = "inverse";

    private static final SeqNoFieldMapper.SequenceIDFields sequenceIDFields = SeqNoFieldMapper.SequenceIDFields.emptySeqID();

    /**
     * Nested aggregations need the {@linkplain DirectoryReader} wrapped.
     */
    @Override
    protected DirectoryReader wrapDirectoryReader(DirectoryReader reader) throws IOException {
        return wrapInMockESDirectoryReader(reader);
    }

    @Override
    protected ScriptService getMockScriptService() {
        Map<String, Function<Map<String, Object>, Object>> scripts = new HashMap<>();
        scripts.put(INVERSE_SCRIPT, vars -> -((Number) vars.get("_value")).doubleValue());
        MockScriptEngine scriptEngine = new MockScriptEngine(MockScriptEngine.NAME, scripts, Collections.emptyMap());
        Map<String, ScriptEngine> engines = Collections.singletonMap(scriptEngine.getType(), scriptEngine);

        return new ScriptService(Settings.EMPTY, engines, ScriptModule.CORE_CONTEXTS, () -> 1L);
    }

    public void testNoDocs() throws IOException {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = newRandomIndexWriterWithLogDocMergePolicy(directory)) {
                // intentionally not writing any docs
            }
            try (DirectoryReader indexReader = wrapInMockESDirectoryReader(DirectoryReader.open(directory))) {
                NestedAggregationBuilder nestedBuilder = new NestedAggregationBuilder(NESTED_AGG, NESTED_OBJECT);
                MaxAggregationBuilder maxAgg = new MaxAggregationBuilder(MAX_AGG_NAME).field(VALUE_FIELD_NAME);
                nestedBuilder.subAggregation(maxAgg);
                MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType(VALUE_FIELD_NAME, NumberFieldMapper.NumberType.LONG);

                InternalNested nested = searchAndReduce(indexReader, new AggTestConfig(nestedBuilder, fieldType));

                assertEquals(NESTED_AGG, nested.getName());
                assertEquals(0, nested.getDocCount());

                Max max = (Max) nested.getProperty(MAX_AGG_NAME);
                assertEquals(MAX_AGG_NAME, max.getName());
                assertEquals(Double.NEGATIVE_INFINITY, max.value(), Double.MIN_VALUE);
                assertFalse(AggregationInspectionHelper.hasValue(nested));
            }
        }
    }

    public void testSingleNestingMax() throws IOException {
        int numRootDocs = randomIntBetween(1, 20);
        int expectedNestedDocs = 0;
        double expectedMaxValue = Double.NEGATIVE_INFINITY;
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = newRandomIndexWriterWithLogDocMergePolicy(directory)) {
                for (int i = 0; i < numRootDocs; i++) {
                    List<Iterable<IndexableField>> documents = new ArrayList<>();
                    int numNestedDocs = randomIntBetween(0, 20);
                    expectedMaxValue = Math.max(
                        expectedMaxValue,
                        generateMaxDocs(documents, numNestedDocs, i, NESTED_OBJECT, VALUE_FIELD_NAME)
                    );
                    expectedNestedDocs += numNestedDocs;

                    LuceneDocument document = new LuceneDocument();
                    document.add(new StringField(IdFieldMapper.NAME, Uid.encodeId(Integer.toString(i)), Field.Store.YES));
                    document.add(new StringField(NestedPathFieldMapper.NAME, "test", Field.Store.NO));
                    sequenceIDFields.addFields(document);
                    documents.add(document);
                    iw.addDocuments(documents);
                }
                iw.commit();
            }
            try (DirectoryReader indexReader = wrapInMockESDirectoryReader(DirectoryReader.open(directory))) {
                NestedAggregationBuilder nestedBuilder = new NestedAggregationBuilder(NESTED_AGG, NESTED_OBJECT);
                MaxAggregationBuilder maxAgg = new MaxAggregationBuilder(MAX_AGG_NAME).field(VALUE_FIELD_NAME);
                nestedBuilder.subAggregation(maxAgg);
                MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType(VALUE_FIELD_NAME, NumberFieldMapper.NumberType.LONG);

                InternalNested nested = searchAndReduce(indexReader, new AggTestConfig(nestedBuilder, fieldType));
                assertEquals(expectedNestedDocs, nested.getDocCount());

                assertEquals(NESTED_AGG, nested.getName());
                assertEquals(expectedNestedDocs, nested.getDocCount());

                Max max = (Max) nested.getProperty(MAX_AGG_NAME);
                assertEquals(MAX_AGG_NAME, max.getName());
                assertEquals(expectedMaxValue, max.value(), Double.MIN_VALUE);

                if (expectedNestedDocs > 0) {
                    assertTrue(AggregationInspectionHelper.hasValue(nested));
                } else {
                    assertFalse(AggregationInspectionHelper.hasValue(nested));
                }
            }
        }
    }

    public void testDoubleNestingMax() throws IOException {
        int numRootDocs = randomIntBetween(1, 20);
        int expectedNestedDocs = 0;
        double expectedMaxValue = Double.NEGATIVE_INFINITY;
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = newRandomIndexWriterWithLogDocMergePolicy(directory)) {
                for (int i = 0; i < numRootDocs; i++) {
                    List<Iterable<IndexableField>> documents = new ArrayList<>();
                    int numNestedDocs = randomIntBetween(0, 20);
                    expectedMaxValue = Math.max(
                        expectedMaxValue,
                        generateMaxDocs(documents, numNestedDocs, i, NESTED_OBJECT + "." + NESTED_OBJECT2, VALUE_FIELD_NAME)
                    );
                    expectedNestedDocs += numNestedDocs;

                    LuceneDocument document = new LuceneDocument();
                    document.add(new StringField(IdFieldMapper.NAME, Uid.encodeId(Integer.toString(i)), Field.Store.YES));
                    document.add(new StringField(NestedPathFieldMapper.NAME, "test", Field.Store.NO));
                    sequenceIDFields.addFields(document);
                    documents.add(document);
                    iw.addDocuments(documents);
                }
                iw.commit();
            }
            try (DirectoryReader indexReader = wrapInMockESDirectoryReader(DirectoryReader.open(directory))) {
                NestedAggregationBuilder nestedBuilder = new NestedAggregationBuilder(NESTED_AGG, NESTED_OBJECT + "." + NESTED_OBJECT2);
                MaxAggregationBuilder maxAgg = new MaxAggregationBuilder(MAX_AGG_NAME).field(VALUE_FIELD_NAME);
                nestedBuilder.subAggregation(maxAgg);

                MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType(VALUE_FIELD_NAME, NumberFieldMapper.NumberType.LONG);

                InternalNested nested = searchAndReduce(indexReader, new AggTestConfig(nestedBuilder, fieldType));
                assertEquals(expectedNestedDocs, nested.getDocCount());

                assertEquals(NESTED_AGG, nested.getName());
                assertEquals(expectedNestedDocs, nested.getDocCount());

                Max max = (Max) nested.getProperty(MAX_AGG_NAME);
                assertEquals(MAX_AGG_NAME, max.getName());
                assertEquals(expectedMaxValue, max.value(), Double.MIN_VALUE);

                if (expectedNestedDocs > 0) {
                    assertTrue(AggregationInspectionHelper.hasValue(nested));
                } else {
                    assertFalse(AggregationInspectionHelper.hasValue(nested));
                }
            }
        }
    }

    public void testOrphanedDocs() throws IOException {
        int numRootDocs = randomIntBetween(1, 20);
        int expectedNestedDocs = 0;
        double expectedSum = 0;
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = newRandomIndexWriterWithLogDocMergePolicy(directory)) {
                for (int i = 0; i < numRootDocs; i++) {
                    List<Iterable<IndexableField>> documents = new ArrayList<>();
                    int numNestedDocs = randomIntBetween(0, 20);
                    expectedSum += generateSumDocs(documents, numNestedDocs, i, NESTED_OBJECT, VALUE_FIELD_NAME);
                    expectedNestedDocs += numNestedDocs;

                    LuceneDocument document = new LuceneDocument();
                    document.add(new StringField(IdFieldMapper.NAME, Uid.encodeId(Integer.toString(i)), Field.Store.YES));
                    document.add(new StringField(NestedPathFieldMapper.NAME, "test", Field.Store.NO));
                    sequenceIDFields.addFields(document);
                    documents.add(document);
                    iw.addDocuments(documents);
                }
                // add some random nested docs that don't belong
                List<Iterable<IndexableField>> documents = new ArrayList<>();
                int numOrphanedDocs = randomIntBetween(0, 20);
                generateSumDocs(documents, numOrphanedDocs, 1234, "foo", VALUE_FIELD_NAME);
                iw.addDocuments(documents);
                iw.commit();
            }
            try (DirectoryReader indexReader = wrapInMockESDirectoryReader(DirectoryReader.open(directory))) {
                NestedAggregationBuilder nestedBuilder = new NestedAggregationBuilder(NESTED_AGG, NESTED_OBJECT);
                SumAggregationBuilder sumAgg = new SumAggregationBuilder(SUM_AGG_NAME).field(VALUE_FIELD_NAME);
                nestedBuilder.subAggregation(sumAgg);
                MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType(VALUE_FIELD_NAME, NumberFieldMapper.NumberType.LONG);

                InternalNested nested = searchAndReduce(indexReader, new AggTestConfig(nestedBuilder, fieldType));
                assertEquals(expectedNestedDocs, nested.getDocCount());

                assertEquals(NESTED_AGG, nested.getName());
                assertEquals(expectedNestedDocs, nested.getDocCount());

                Sum sum = (Sum) ((InternalAggregation) nested).getProperty(SUM_AGG_NAME);
                assertEquals(SUM_AGG_NAME, sum.getName());
                assertEquals(expectedSum, sum.value(), Double.MIN_VALUE);
            }
        }
    }

    public void testResetRootDocId() throws Exception {
        IndexWriterConfig iwc = new IndexWriterConfig(null).setMergePolicy(new LogDocMergePolicy());
        iwc.setMergePolicy(NoMergePolicy.INSTANCE);
        SeqNoFieldMapper.SequenceIDFields sequenceIDFields = SeqNoFieldMapper.SequenceIDFields.emptySeqID();
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = new RandomIndexWriter(random(), directory, iwc)) {
                List<Iterable<IndexableField>> documents = new ArrayList<>();

                // 1 segment with, 1 root document, with 3 nested sub docs
                LuceneDocument document = new LuceneDocument();
                document.add(new StringField(IdFieldMapper.NAME, Uid.encodeId("1"), Field.Store.NO));
                document.add(new StringField(NestedPathFieldMapper.NAME, "nested_field", Field.Store.NO));
                documents.add(document);
                document = new LuceneDocument();
                document.add(new StringField(IdFieldMapper.NAME, Uid.encodeId("1"), Field.Store.NO));
                document.add(new StringField(NestedPathFieldMapper.NAME, "nested_field", Field.Store.NO));
                documents.add(document);
                document = new LuceneDocument();
                document.add(new StringField(IdFieldMapper.NAME, Uid.encodeId("1"), Field.Store.NO));
                document.add(new StringField(NestedPathFieldMapper.NAME, "nested_field", Field.Store.NO));
                documents.add(document);
                document = new LuceneDocument();
                document.add(new StringField(IdFieldMapper.NAME, Uid.encodeId("1"), Field.Store.YES));
                document.add(new StringField(NestedPathFieldMapper.NAME, "test", Field.Store.NO));
                sequenceIDFields.addFields(document);
                documents.add(document);
                iw.addDocuments(documents);
                iw.commit();

                documents.clear();
                // 1 segment with:
                // 1 document, with 1 nested subdoc
                document = new LuceneDocument();
                document.add(new StringField(IdFieldMapper.NAME, Uid.encodeId("2"), Field.Store.NO));
                document.add(new StringField(NestedPathFieldMapper.NAME, "nested_field", Field.Store.NO));
                documents.add(document);
                document = new LuceneDocument();
                document.add(new StringField(IdFieldMapper.NAME, Uid.encodeId("2"), Field.Store.YES));
                document.add(new StringField(NestedPathFieldMapper.NAME, "test", Field.Store.NO));
                sequenceIDFields.addFields(document);
                documents.add(document);
                iw.addDocuments(documents);
                documents.clear();
                // and 1 document, with 1 nested subdoc
                document = new LuceneDocument();
                document.add(new StringField(IdFieldMapper.NAME, Uid.encodeId("3"), Field.Store.NO));
                document.add(new StringField(NestedPathFieldMapper.NAME, "nested_field", Field.Store.NO));
                documents.add(document);
                document = new LuceneDocument();
                document.add(new StringField(IdFieldMapper.NAME, Uid.encodeId("3"), Field.Store.YES));
                document.add(new StringField(NestedPathFieldMapper.NAME, "test", Field.Store.NO));
                sequenceIDFields.addFields(document);
                documents.add(document);
                iw.addDocuments(documents);

                iw.commit();
                iw.close();
            }
            try (DirectoryReader indexReader = wrapInMockESDirectoryReader(DirectoryReader.open(directory))) {

                NestedAggregationBuilder nestedBuilder = new NestedAggregationBuilder(NESTED_AGG, "nested_field");
                MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType(VALUE_FIELD_NAME, NumberFieldMapper.NumberType.LONG);

                BooleanQuery.Builder bq = new BooleanQuery.Builder();
                bq.add(Queries.newNonNestedFilter(IndexVersion.current()), BooleanClause.Occur.MUST);
                bq.add(new TermQuery(new Term(IdFieldMapper.NAME, Uid.encodeId("2"))), BooleanClause.Occur.MUST_NOT);

                InternalNested nested = searchAndReduce(
                    indexReader,
                    new AggTestConfig(nestedBuilder, fieldType).withQuery(new ConstantScoreQuery(bq.build()))
                );

                assertEquals(NESTED_AGG, nested.getName());
                // The bug manifests if 6 docs are returned, because currentRootDoc isn't reset the previous child docs from the first
                // segment are emitted as hits.
                assertEquals(4L, nested.getDocCount());

                assertTrue(AggregationInspectionHelper.hasValue(nested));
            }
        }
    }

    public void testNestedOrdering() throws IOException {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = newRandomIndexWriterWithLogDocMergePolicy(directory)) {
                iw.addDocuments(generateBook("1", new String[] { "a" }, new int[] { 12, 13, 14 }));
                iw.addDocuments(generateBook("2", new String[] { "b" }, new int[] { 5, 50 }));
                iw.addDocuments(generateBook("3", new String[] { "c" }, new int[] { 39, 19 }));
                iw.addDocuments(generateBook("4", new String[] { "d" }, new int[] { 2, 1, 3 }));
                iw.addDocuments(generateBook("5", new String[] { "a" }, new int[] { 70, 10 }));
                iw.addDocuments(generateBook("6", new String[] { "e" }, new int[] { 23, 21 }));
                iw.addDocuments(generateBook("7", new String[] { "e", "a" }, new int[] { 8, 8 }));
                iw.addDocuments(generateBook("8", new String[] { "f" }, new int[] { 12, 14 }));
                iw.addDocuments(generateBook("9", new String[] { "g", "c", "e" }, new int[] { 18, 8 }));
            }
            try (DirectoryReader indexReader = wrapInMockESDirectoryReader(DirectoryReader.open(directory))) {
                MappedFieldType fieldType1 = new NumberFieldMapper.NumberFieldType("num_pages", NumberFieldMapper.NumberType.LONG);
                MappedFieldType fieldType2 = new KeywordFieldMapper.KeywordFieldType("author");

                TermsAggregationBuilder termsBuilder = new TermsAggregationBuilder("authors").userValueTypeHint(ValueType.STRING)
                    .field("author")
                    .order(BucketOrder.aggregation("chapters>num_pages.value", true));
                NestedAggregationBuilder nestedBuilder = new NestedAggregationBuilder("chapters", "nested_chapters");
                MaxAggregationBuilder maxAgg = new MaxAggregationBuilder("num_pages").field("num_pages");
                nestedBuilder.subAggregation(maxAgg);
                termsBuilder.subAggregation(nestedBuilder);

                Terms terms = searchAndReduce(indexReader, new AggTestConfig(termsBuilder, fieldType1, fieldType2));

                assertEquals(7, terms.getBuckets().size());
                assertEquals("authors", terms.getName());

                Terms.Bucket bucket = terms.getBuckets().get(0);
                assertEquals("d", bucket.getKeyAsString());
                Max numPages = ((Nested) bucket.getAggregations().get("chapters")).getAggregations().get("num_pages");
                assertEquals(3, (int) numPages.value());

                bucket = terms.getBuckets().get(1);
                assertEquals("f", bucket.getKeyAsString());
                numPages = ((Nested) bucket.getAggregations().get("chapters")).getAggregations().get("num_pages");
                assertEquals(14, (int) numPages.value());

                bucket = terms.getBuckets().get(2);
                assertEquals("g", bucket.getKeyAsString());
                numPages = ((Nested) bucket.getAggregations().get("chapters")).getAggregations().get("num_pages");
                assertEquals(18, (int) numPages.value());

                bucket = terms.getBuckets().get(3);
                assertEquals("e", bucket.getKeyAsString());
                numPages = ((Nested) bucket.getAggregations().get("chapters")).getAggregations().get("num_pages");
                assertEquals(23, (int) numPages.value());

                bucket = terms.getBuckets().get(4);
                assertEquals("c", bucket.getKeyAsString());
                numPages = ((Nested) bucket.getAggregations().get("chapters")).getAggregations().get("num_pages");
                assertEquals(39, (int) numPages.value());

                bucket = terms.getBuckets().get(5);
                assertEquals("b", bucket.getKeyAsString());
                numPages = ((Nested) bucket.getAggregations().get("chapters")).getAggregations().get("num_pages");
                assertEquals(50, (int) numPages.value());

                bucket = terms.getBuckets().get(6);
                assertEquals("a", bucket.getKeyAsString());
                numPages = ((Nested) bucket.getAggregations().get("chapters")).getAggregations().get("num_pages");
                assertEquals(70, (int) numPages.value());

                // reverse order:
                termsBuilder = new TermsAggregationBuilder("authors").userValueTypeHint(ValueType.STRING)
                    .field("author")
                    .order(BucketOrder.aggregation("chapters>num_pages.value", false));
                nestedBuilder = new NestedAggregationBuilder("chapters", "nested_chapters");
                maxAgg = new MaxAggregationBuilder("num_pages").field("num_pages");
                nestedBuilder.subAggregation(maxAgg);
                termsBuilder.subAggregation(nestedBuilder);

                terms = searchAndReduce(indexReader, new AggTestConfig(termsBuilder, fieldType1, fieldType2));

                assertEquals(7, terms.getBuckets().size());
                assertEquals("authors", terms.getName());

                bucket = terms.getBuckets().get(0);
                assertEquals("a", bucket.getKeyAsString());
                numPages = ((Nested) bucket.getAggregations().get("chapters")).getAggregations().get("num_pages");
                assertEquals(70, (int) numPages.value());

                bucket = terms.getBuckets().get(1);
                assertEquals("b", bucket.getKeyAsString());
                numPages = ((Nested) bucket.getAggregations().get("chapters")).getAggregations().get("num_pages");
                assertEquals(50, (int) numPages.value());

                bucket = terms.getBuckets().get(2);
                assertEquals("c", bucket.getKeyAsString());
                numPages = ((Nested) bucket.getAggregations().get("chapters")).getAggregations().get("num_pages");
                assertEquals(39, (int) numPages.value());

                bucket = terms.getBuckets().get(3);
                assertEquals("e", bucket.getKeyAsString());
                numPages = ((Nested) bucket.getAggregations().get("chapters")).getAggregations().get("num_pages");
                assertEquals(23, (int) numPages.value());

                bucket = terms.getBuckets().get(4);
                assertEquals("g", bucket.getKeyAsString());
                numPages = ((Nested) bucket.getAggregations().get("chapters")).getAggregations().get("num_pages");
                assertEquals(18, (int) numPages.value());

                bucket = terms.getBuckets().get(5);
                assertEquals("f", bucket.getKeyAsString());
                numPages = ((Nested) bucket.getAggregations().get("chapters")).getAggregations().get("num_pages");
                assertEquals(14, (int) numPages.value());

                bucket = terms.getBuckets().get(6);
                assertEquals("d", bucket.getKeyAsString());
                numPages = ((Nested) bucket.getAggregations().get("chapters")).getAggregations().get("num_pages");
                assertEquals(3, (int) numPages.value());
            }
        }
    }

    public void testNestedOrdering_random() throws IOException {
        int numBooks = randomIntBetween(32, 512);
        List<Tuple<String, int[]>> books = new ArrayList<>();
        for (int i = 0; i < numBooks; i++) {
            int numChapters = randomIntBetween(1, 8);
            int[] chapters = new int[numChapters];
            for (int j = 0; j < numChapters; j++) {
                chapters[j] = randomIntBetween(2, 64);
            }
            books.add(Tuple.tuple(Strings.format("%03d", i), chapters));
        }
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = newRandomIndexWriterWithLogDocMergePolicy(directory)) {
                int id = 0;
                for (Tuple<String, int[]> book : books) {
                    iw.addDocuments(generateBook(Strings.format("%03d", id), new String[] { book.v1() }, book.v2()));
                    id++;
                }
            }
            for (Tuple<String, int[]> book : books) {
                Arrays.sort(book.v2());
            }
            books.sort((o1, o2) -> {
                int cmp = Integer.compare(o1.v2()[0], o2.v2()[0]);
                if (cmp == 0) {
                    return o1.v1().compareTo(o2.v1());
                } else {
                    return cmp;
                }
            });
            try (DirectoryReader indexReader = wrapInMockESDirectoryReader(DirectoryReader.open(directory))) {
                MappedFieldType fieldType1 = new NumberFieldMapper.NumberFieldType("num_pages", NumberFieldMapper.NumberType.LONG);
                MappedFieldType fieldType2 = new KeywordFieldMapper.KeywordFieldType("author");

                TermsAggregationBuilder termsBuilder = new TermsAggregationBuilder("authors").userValueTypeHint(ValueType.STRING)
                    .size(books.size())
                    .field("author")
                    .order(BucketOrder.compound(BucketOrder.aggregation("chapters>num_pages.value", true), BucketOrder.key(true)));
                NestedAggregationBuilder nestedBuilder = new NestedAggregationBuilder("chapters", "nested_chapters");
                MinAggregationBuilder minAgg = new MinAggregationBuilder("num_pages").field("num_pages");
                nestedBuilder.subAggregation(minAgg);
                termsBuilder.subAggregation(nestedBuilder);

                AggTestConfig aggTestConfig = new AggTestConfig(termsBuilder, fieldType1, fieldType2);
                Terms terms = searchAndReduce(indexReader, aggTestConfig);

                assertEquals(books.size(), terms.getBuckets().size());
                assertEquals("authors", terms.getName());

                for (int i = 0; i < books.size(); i++) {
                    Tuple<String, int[]> book = books.get(i);
                    Terms.Bucket bucket = terms.getBuckets().get(i);
                    assertEquals(book.v1(), bucket.getKeyAsString());
                    Min numPages = ((Nested) bucket.getAggregations().get("chapters")).getAggregations().get("num_pages");
                    assertEquals(book.v2()[0], (int) numPages.value());
                }
            }
        }
    }

    public void testPreGetChildLeafCollectors() throws IOException {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = newRandomIndexWriterWithLogDocMergePolicy(directory)) {
                List<Iterable<IndexableField>> documents = new ArrayList<>();
                LuceneDocument document = new LuceneDocument();
                document.add(new StringField(IdFieldMapper.NAME, Uid.encodeId("1"), Field.Store.NO));
                document.add(new StringField(NestedPathFieldMapper.NAME, "nested_field", Field.Store.NO));
                document.add(new SortedDocValuesField("key", new BytesRef("key1")));
                document.add(new SortedDocValuesField("value", new BytesRef("a1")));
                documents.add(document);
                document = new LuceneDocument();
                document.add(new StringField(IdFieldMapper.NAME, Uid.encodeId("1"), Field.Store.NO));
                document.add(new StringField(NestedPathFieldMapper.NAME, "nested_field", Field.Store.NO));
                document.add(new SortedDocValuesField("key", new BytesRef("key2")));
                document.add(new SortedDocValuesField("value", new BytesRef("b1")));
                documents.add(document);
                document = new LuceneDocument();
                document.add(new StringField(IdFieldMapper.NAME, Uid.encodeId("1"), Field.Store.YES));
                document.add(new StringField(NestedPathFieldMapper.NAME, "_doc", Field.Store.NO));
                sequenceIDFields.addFields(document);
                documents.add(document);
                iw.addDocuments(documents);
                iw.commit();
                documents.clear();

                document = new LuceneDocument();
                document.add(new StringField(IdFieldMapper.NAME, Uid.encodeId("2"), Field.Store.NO));
                document.add(new StringField(NestedPathFieldMapper.NAME, "nested_field", Field.Store.NO));
                document.add(new SortedDocValuesField("key", new BytesRef("key1")));
                document.add(new SortedDocValuesField("value", new BytesRef("a2")));
                documents.add(document);
                document = new LuceneDocument();
                document.add(new StringField(IdFieldMapper.NAME, Uid.encodeId("2"), Field.Store.NO));
                document.add(new StringField(NestedPathFieldMapper.NAME, "nested_field", Field.Store.NO));
                document.add(new SortedDocValuesField("key", new BytesRef("key2")));
                document.add(new SortedDocValuesField("value", new BytesRef("b2")));
                documents.add(document);
                document = new LuceneDocument();
                document.add(new StringField(IdFieldMapper.NAME, Uid.encodeId("2"), Field.Store.YES));
                document.add(new StringField(NestedPathFieldMapper.NAME, "_doc", Field.Store.NO));
                sequenceIDFields.addFields(document);
                documents.add(document);
                iw.addDocuments(documents);
                iw.commit();
                documents.clear();

                document = new LuceneDocument();
                document.add(new StringField(IdFieldMapper.NAME, Uid.encodeId("3"), Field.Store.NO));
                document.add(new StringField(NestedPathFieldMapper.NAME, "nested_field", Field.Store.NO));
                document.add(new SortedDocValuesField("key", new BytesRef("key1")));
                document.add(new SortedDocValuesField("value", new BytesRef("a3")));
                documents.add(document);
                document = new LuceneDocument();
                document.add(new StringField(IdFieldMapper.NAME, Uid.encodeId("3"), Field.Store.NO));
                document.add(new StringField(NestedPathFieldMapper.NAME, "nested_field", Field.Store.NO));
                document.add(new SortedDocValuesField("key", new BytesRef("key2")));
                document.add(new SortedDocValuesField("value", new BytesRef("b3")));
                documents.add(document);
                document = new LuceneDocument();
                document.add(new StringField(IdFieldMapper.NAME, Uid.encodeId("3"), Field.Store.YES));
                document.add(new StringField(NestedPathFieldMapper.NAME, "_doc", Field.Store.NO));
                sequenceIDFields.addFields(document);
                documents.add(document);
                iw.addDocuments(documents);
                iw.commit();
            }
            try (DirectoryReader indexReader = wrapInMockESDirectoryReader(DirectoryReader.open(directory))) {
                TermsAggregationBuilder valueBuilder = new TermsAggregationBuilder("value").userValueTypeHint(ValueType.STRING)
                    .field("value");
                TermsAggregationBuilder keyBuilder = new TermsAggregationBuilder("key").userValueTypeHint(ValueType.STRING).field("key");
                keyBuilder.subAggregation(valueBuilder);
                NestedAggregationBuilder nestedBuilder = new NestedAggregationBuilder(NESTED_AGG, "nested_field");
                nestedBuilder.subAggregation(keyBuilder);
                FilterAggregationBuilder filterAggregationBuilder = new FilterAggregationBuilder("filterAgg", new MatchAllQueryBuilder());
                filterAggregationBuilder.subAggregation(nestedBuilder);

                MappedFieldType fieldType1 = new KeywordFieldMapper.KeywordFieldType("key");
                MappedFieldType fieldType2 = new KeywordFieldMapper.KeywordFieldType("value");

                Filter filter = searchAndReduce(
                    indexReader,
                    new AggTestConfig(filterAggregationBuilder, fieldType1, fieldType2).withQuery(
                        Queries.newNonNestedFilter(IndexVersion.current())
                    )
                );

                assertEquals("filterAgg", filter.getName());
                assertEquals(3L, filter.getDocCount());

                InternalNested nested = filter.getAggregations().get(NESTED_AGG);
                assertEquals(6L, nested.getDocCount());

                StringTerms keyAgg = nested.getAggregations().get("key");
                assertEquals(2, keyAgg.getBuckets().size());
                Terms.Bucket key1 = keyAgg.getBuckets().get(0);
                assertEquals("key1", key1.getKey());
                StringTerms valueAgg = key1.getAggregations().get("value");
                assertEquals(3, valueAgg.getBuckets().size());
                assertEquals("a1", valueAgg.getBuckets().get(0).getKey());
                assertEquals("a2", valueAgg.getBuckets().get(1).getKey());
                assertEquals("a3", valueAgg.getBuckets().get(2).getKey());

                Terms.Bucket key2 = keyAgg.getBuckets().get(1);
                assertEquals("key2", key2.getKey());
                valueAgg = key2.getAggregations().get("value");
                assertEquals(3, valueAgg.getBuckets().size());
                assertEquals("b1", valueAgg.getBuckets().get(0).getKey());
                assertEquals("b2", valueAgg.getBuckets().get(1).getKey());
                assertEquals("b3", valueAgg.getBuckets().get(2).getKey());
            }
        }
    }

    public void testFieldAlias() throws IOException {
        int numRootDocs = randomIntBetween(1, 20);
        int expectedNestedDocs = 0;

        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType(VALUE_FIELD_NAME, NumberFieldMapper.NumberType.LONG);

        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = newRandomIndexWriterWithLogDocMergePolicy(directory)) {
                for (int i = 0; i < numRootDocs; i++) {
                    List<Iterable<IndexableField>> documents = new ArrayList<>();
                    int numNestedDocs = randomIntBetween(0, 20);
                    expectedNestedDocs += numNestedDocs;
                    generateDocuments(documents, numNestedDocs, i, NESTED_OBJECT, VALUE_FIELD_NAME, (doc, n) -> {});

                    LuceneDocument document = new LuceneDocument();
                    document.add(new StringField(IdFieldMapper.NAME, Uid.encodeId(Integer.toString(i)), Field.Store.YES));
                    document.add(new StringField(NestedPathFieldMapper.NAME, "test", Field.Store.NO));
                    sequenceIDFields.addFields(document);
                    documents.add(document);
                    iw.addDocuments(documents);
                }
                iw.commit();
            }

            try (DirectoryReader indexReader = wrapInMockESDirectoryReader(DirectoryReader.open(directory))) {
                NestedAggregationBuilder agg = nested(NESTED_AGG, NESTED_OBJECT).subAggregation(max(MAX_AGG_NAME).field(VALUE_FIELD_NAME));
                NestedAggregationBuilder aliasAgg = nested(NESTED_AGG, NESTED_OBJECT).subAggregation(
                    max(MAX_AGG_NAME).field(VALUE_FIELD_NAME + "-alias")
                );

                InternalNested nested = searchAndReduce(indexReader, new AggTestConfig(agg, fieldType));
                Nested aliasNested = searchAndReduce(indexReader, new AggTestConfig(aliasAgg, fieldType));

                assertEquals(nested, aliasNested);
                assertEquals(expectedNestedDocs, nested.getDocCount());
            }
        }
    }

    /**
     * This tests to make sure pipeline aggs embedded under a SingleBucket agg (like nested)
     * are properly reduced
     */
    public void testNestedWithPipeline() throws IOException {
        int numRootDocs = randomIntBetween(1, 20);
        int expectedNestedDocs = 0;
        double expectedMaxValue = Double.NEGATIVE_INFINITY;
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = newRandomIndexWriterWithLogDocMergePolicy(directory)) {
                for (int i = 0; i < numRootDocs; i++) {
                    List<Iterable<IndexableField>> documents = new ArrayList<>();
                    expectedMaxValue = Math.max(expectedMaxValue, generateMaxDocs(documents, 1, i, NESTED_OBJECT, VALUE_FIELD_NAME));
                    expectedNestedDocs += 1;

                    LuceneDocument document = new LuceneDocument();
                    document.add(new StringField(IdFieldMapper.NAME, Uid.encodeId(Integer.toString(i)), Field.Store.YES));
                    document.add(new StringField(NestedPathFieldMapper.NAME, "test", Field.Store.NO));
                    sequenceIDFields.addFields(document);
                    documents.add(document);
                    iw.addDocuments(documents);
                }
                iw.commit();
            }
            try (DirectoryReader indexReader = wrapInMockESDirectoryReader(DirectoryReader.open(directory))) {
                NestedAggregationBuilder nestedBuilder = new NestedAggregationBuilder(NESTED_AGG, NESTED_OBJECT).subAggregation(
                    new TermsAggregationBuilder("terms").field(VALUE_FIELD_NAME)
                        .userValueTypeHint(ValueType.NUMERIC)
                        .subAggregation(new MaxAggregationBuilder(MAX_AGG_NAME).field(VALUE_FIELD_NAME))
                        .subAggregation(
                            new BucketScriptPipelineAggregationBuilder(
                                "bucketscript",
                                Collections.singletonMap("_value", MAX_AGG_NAME),
                                new Script(ScriptType.INLINE, MockScriptEngine.NAME, INVERSE_SCRIPT, Collections.emptyMap())
                            )
                        )
                );

                MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType(VALUE_FIELD_NAME, NumberFieldMapper.NumberType.LONG);

                InternalNested nested = searchAndReduce(indexReader, new AggTestConfig(nestedBuilder, fieldType));

                assertEquals(expectedNestedDocs, nested.getDocCount());
                assertEquals(NESTED_AGG, nested.getName());
                assertEquals(expectedNestedDocs, nested.getDocCount());

                @SuppressWarnings("unchecked")
                InternalTerms<?, LongTerms.Bucket> terms = (InternalTerms<?, LongTerms.Bucket>) nested.getProperty("terms");
                assertNotNull(terms);

                for (LongTerms.Bucket bucket : terms.getBuckets()) {
                    Max max = (Max) bucket.getAggregations().asMap().get(MAX_AGG_NAME);
                    InternalSimpleValue bucketScript = (InternalSimpleValue) bucket.getAggregations().asMap().get("bucketscript");
                    assertNotNull(max);
                    assertNotNull(bucketScript);
                    assertEquals(max.value(), -bucketScript.getValue(), Double.MIN_VALUE);
                }

                assertTrue(AggregationInspectionHelper.hasValue(nested));
            }
        }
    }

    public void testNestedUnderTerms() throws IOException {
        int numProducts = scaledRandomIntBetween(1, 100);
        int numResellers = scaledRandomIntBetween(1, 100);

        AggregationBuilder b = new TermsAggregationBuilder("products").field("product_id")
            .size(numProducts)
            .subAggregation(
                new NestedAggregationBuilder("nested", "nested_reseller").subAggregation(
                    new TermsAggregationBuilder("resellers").field("reseller_id").size(numResellers)
                )
            );
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = newRandomIndexWriterWithLogDocMergePolicy(directory)) {
                buildResellerData(numProducts, numResellers).accept(iw);
            }
            try (DirectoryReader indexReader = wrapInMockESDirectoryReader(DirectoryReader.open(directory))) {
                LongTerms products = searchAndReduce(indexReader, new AggTestConfig(b, resellersMappedFields()));
                assertThat(
                    products.getBuckets().stream().map(LongTerms.Bucket::getKeyAsNumber).collect(toList()),
                    equalTo(LongStream.range(0, numProducts).mapToObj(Long::valueOf).collect(toList()))
                );
                for (int p = 0; p < numProducts; p++) {
                    LongTerms.Bucket bucket = products.getBucketByKey(Integer.toString(p));
                    assertThat(bucket.getDocCount(), equalTo(1L));
                    InternalNested nested = bucket.getAggregations().get("nested");
                    assertThat(nested.getDocCount(), equalTo((long) numResellers));
                    LongTerms resellers = nested.getAggregations().get("resellers");
                    assertThat(
                        resellers.getBuckets().stream().map(LongTerms.Bucket::getKeyAsNumber).collect(toList()),
                        equalTo(LongStream.range(0, numResellers).mapToObj(Long::valueOf).collect(toList()))
                    );
                }
            }
        }
    }

    public static CheckedConsumer<RandomIndexWriter, IOException> buildResellerData(int numProducts, int numResellers) {
        return iw -> {
            for (int p = 0; p < numProducts; p++) {
                List<Iterable<IndexableField>> documents = new ArrayList<>();
                generateDocuments(
                    documents,
                    numResellers,
                    p,
                    "nested_reseller",
                    "value",
                    (doc, n) -> doc.add(new SortedNumericDocValuesField("reseller_id", n))
                );
                LuceneDocument document = new LuceneDocument();
                document.add(new StringField(IdFieldMapper.NAME, Uid.encodeId(Integer.toString(p)), Field.Store.YES));
                document.add(new StringField(NestedPathFieldMapper.NAME, "test", Field.Store.NO));
                sequenceIDFields.addFields(document);
                document.add(new SortedNumericDocValuesField("product_id", p));
                documents.add(document);
                iw.addDocuments(documents);
            }
        };
    }

    public static MappedFieldType[] resellersMappedFields() {
        MappedFieldType productIdField = new NumberFieldMapper.NumberFieldType("product_id", NumberFieldMapper.NumberType.LONG);
        MappedFieldType resellerIdField = new NumberFieldMapper.NumberFieldType("reseller_id", NumberFieldMapper.NumberType.LONG);
        return new MappedFieldType[] { productIdField, resellerIdField };
    }

    private double generateMaxDocs(List<Iterable<IndexableField>> documents, int numNestedDocs, int id, String path, String fieldName) {
        return DoubleStream.of(generateDocuments(documents, numNestedDocs, id, path, fieldName, (doc, n) -> {}))
            .max()
            .orElse(Double.NEGATIVE_INFINITY);
    }

    private double generateSumDocs(List<Iterable<IndexableField>> documents, int numNestedDocs, int id, String path, String fieldName) {
        return DoubleStream.of(generateDocuments(documents, numNestedDocs, id, path, fieldName, (doc, n) -> {})).sum();
    }

    private static double[] generateDocuments(
        List<Iterable<IndexableField>> documents,
        int numNestedDocs,
        int id,
        String path,
        String fieldName,
        BiConsumer<Document, Integer> extra
    ) {
        double[] values = new double[numNestedDocs];
        for (int nested = 0; nested < numNestedDocs; nested++) {
            Document document = new Document();
            document.add(new StringField(IdFieldMapper.NAME, Uid.encodeId(Integer.toString(id)), Field.Store.NO));
            document.add(new StringField(NestedPathFieldMapper.NAME, path, Field.Store.NO));
            long value = randomNonNegativeLong() % 10000;
            document.add(new SortedNumericDocValuesField(fieldName, value));
            extra.accept(document, nested);
            documents.add(document);
            values[nested] = value;
        }
        return values;
    }

    public static List<Iterable<IndexableField>> generateBook(String id, String[] authors, int[] numPages) {
        List<Iterable<IndexableField>> documents = new ArrayList<>();

        for (int numPage : numPages) {
            Document document = new Document();
            document.add(new StringField(IdFieldMapper.NAME, Uid.encodeId(id), Field.Store.NO));
            document.add(new StringField(NestedPathFieldMapper.NAME, "nested_chapters", Field.Store.NO));
            document.add(new SortedNumericDocValuesField("num_pages", numPage));
            documents.add(document);
        }

        LuceneDocument document = new LuceneDocument();
        document.add(new StringField(IdFieldMapper.NAME, Uid.encodeId(id), Field.Store.YES));
        document.add(new StringField(NestedPathFieldMapper.NAME, "book", Field.Store.NO));
        sequenceIDFields.addFields(document);
        for (String author : authors) {
            document.add(new KeywordField("author", new BytesRef(author), Field.Store.NO));
        }
        documents.add(document);

        return documents;
    }

    @Override
    protected List<ObjectMapper> objectMappers() {
        return MOCK_OBJECT_MAPPERS;
    }

    static final List<ObjectMapper> MOCK_OBJECT_MAPPERS = List.of(
        nestedObject(NESTED_OBJECT),
        nestedObject(NESTED_OBJECT + "." + NESTED_OBJECT2),
        nestedObject("nested_reseller"),
        nestedObject("nested_chapters"),
        nestedObject("nested_field")
    );

    public static NestedObjectMapper nestedObject(String path) {
        return new NestedObjectMapper.Builder(path, IndexVersion.current()).build(MapperBuilderContext.root(false, false));
    }
}
