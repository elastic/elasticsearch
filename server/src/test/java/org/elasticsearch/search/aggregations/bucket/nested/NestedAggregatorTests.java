/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.aggregations.bucket.nested;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.mapper.TypeFieldMapper;
import org.elasticsearch.index.mapper.UidFieldMapper;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.bucket.filter.Filter;
import org.elasticsearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.max.InternalMax;
import org.elasticsearch.search.aggregations.metrics.max.Max;
import org.elasticsearch.search.aggregations.metrics.max.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.min.Min;
import org.elasticsearch.search.aggregations.metrics.min.MinAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.sum.InternalSum;
import org.elasticsearch.search.aggregations.metrics.sum.SumAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.test.VersionUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.stream.DoubleStream;

public class NestedAggregatorTests extends AggregatorTestCase {

    private static final String VALUE_FIELD_NAME = "number";
    private static final String NESTED_OBJECT = "nested_object";
    private static final String NESTED_OBJECT2 = "nested_object2";
    private static final String NESTED_AGG = "nestedAgg";
    private static final String MAX_AGG_NAME = "maxAgg";
    private static final String SUM_AGG_NAME = "sumAgg";

    private final SeqNoFieldMapper.SequenceIDFields sequenceIDFields = SeqNoFieldMapper.SequenceIDFields.emptySeqID();


    public void testNoDocs() throws IOException {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
                // intentionally not writing any docs
            }
            try (IndexReader indexReader = wrap(DirectoryReader.open(directory))) {
                NestedAggregationBuilder nestedBuilder = new NestedAggregationBuilder(NESTED_AGG,
                    NESTED_OBJECT);
                MaxAggregationBuilder maxAgg = new MaxAggregationBuilder(MAX_AGG_NAME)
                    .field(VALUE_FIELD_NAME);
                nestedBuilder.subAggregation(maxAgg);
                MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType(
                    NumberFieldMapper.NumberType.LONG);
                fieldType.setName(VALUE_FIELD_NAME);

                Nested nested = search(newSearcher(indexReader, false, true),
                    new MatchAllDocsQuery(), nestedBuilder, fieldType);

                assertEquals(NESTED_AGG, nested.getName());
                assertEquals(0, nested.getDocCount());

                InternalMax max = (InternalMax)
                    ((InternalAggregation)nested).getProperty(MAX_AGG_NAME);
                assertEquals(MAX_AGG_NAME, max.getName());
                assertEquals(Double.NEGATIVE_INFINITY, max.getValue(), Double.MIN_VALUE);
            }
        }
    }

    public void testSingleNestingMax() throws IOException {
        int numRootDocs = randomIntBetween(1, 20);
        int expectedNestedDocs = 0;
        double expectedMaxValue = Double.NEGATIVE_INFINITY;
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
                for (int i = 0; i < numRootDocs; i++) {
                    List<Document> documents = new ArrayList<>();
                    int numNestedDocs = randomIntBetween(0, 20);
                    expectedMaxValue = Math.max(expectedMaxValue,
                        generateMaxDocs(documents, numNestedDocs, i, NESTED_OBJECT, VALUE_FIELD_NAME));
                    expectedNestedDocs += numNestedDocs;

                    Document document = new Document();
                    document.add(new Field(UidFieldMapper.NAME, "type#" + i,
                        UidFieldMapper.Defaults.FIELD_TYPE));
                    document.add(new Field(TypeFieldMapper.NAME, "test",
                        TypeFieldMapper.Defaults.FIELD_TYPE));
                    document.add(sequenceIDFields.primaryTerm);
                    documents.add(document);
                    iw.addDocuments(documents);
                }
                iw.commit();
            }
            try (IndexReader indexReader = wrap(DirectoryReader.open(directory))) {
                NestedAggregationBuilder nestedBuilder = new NestedAggregationBuilder(NESTED_AGG,
                    NESTED_OBJECT);
                MaxAggregationBuilder maxAgg = new MaxAggregationBuilder(MAX_AGG_NAME)
                    .field(VALUE_FIELD_NAME);
                nestedBuilder.subAggregation(maxAgg);
                MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType(
                    NumberFieldMapper.NumberType.LONG);
                fieldType.setName(VALUE_FIELD_NAME);

                Nested nested = search(newSearcher(indexReader, false, true),
                    new MatchAllDocsQuery(), nestedBuilder, fieldType);
                assertEquals(expectedNestedDocs, nested.getDocCount());

                assertEquals(NESTED_AGG, nested.getName());
                assertEquals(expectedNestedDocs, nested.getDocCount());

                InternalMax max = (InternalMax)
                    ((InternalAggregation)nested).getProperty(MAX_AGG_NAME);
                assertEquals(MAX_AGG_NAME, max.getName());
                assertEquals(expectedMaxValue, max.getValue(), Double.MIN_VALUE);
            }
        }
    }

    public void testDoubleNestingMax() throws IOException {
        int numRootDocs = randomIntBetween(1, 20);
        int expectedNestedDocs = 0;
        double expectedMaxValue = Double.NEGATIVE_INFINITY;
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
                for (int i = 0; i < numRootDocs; i++) {
                    List<Document> documents = new ArrayList<>();
                    int numNestedDocs = randomIntBetween(0, 20);
                    expectedMaxValue = Math.max(expectedMaxValue,
                        generateMaxDocs(documents, numNestedDocs, i, NESTED_OBJECT + "." + NESTED_OBJECT2, VALUE_FIELD_NAME));
                    expectedNestedDocs += numNestedDocs;

                    Document document = new Document();
                    document.add(new Field(UidFieldMapper.NAME, "type#" + i,
                        UidFieldMapper.Defaults.FIELD_TYPE));
                    document.add(new Field(TypeFieldMapper.NAME, "test",
                        TypeFieldMapper.Defaults.FIELD_TYPE));
                    document.add(sequenceIDFields.primaryTerm);
                    documents.add(document);
                    iw.addDocuments(documents);
                }
                iw.commit();
            }
            try (IndexReader indexReader = wrap(DirectoryReader.open(directory))) {
                NestedAggregationBuilder nestedBuilder = new NestedAggregationBuilder(NESTED_AGG,
                    NESTED_OBJECT + "." + NESTED_OBJECT2);
                MaxAggregationBuilder maxAgg = new MaxAggregationBuilder(MAX_AGG_NAME)
                    .field(VALUE_FIELD_NAME);
                nestedBuilder.subAggregation(maxAgg);

                MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType(
                    NumberFieldMapper.NumberType.LONG);
                fieldType.setName(VALUE_FIELD_NAME);

                Nested nested = search(newSearcher(indexReader, false, true),
                    new MatchAllDocsQuery(), nestedBuilder, fieldType);
                assertEquals(expectedNestedDocs, nested.getDocCount());

                assertEquals(NESTED_AGG, nested.getName());
                assertEquals(expectedNestedDocs, nested.getDocCount());

                InternalMax max = (InternalMax)
                    ((InternalAggregation)nested).getProperty(MAX_AGG_NAME);
                assertEquals(MAX_AGG_NAME, max.getName());
                assertEquals(expectedMaxValue, max.getValue(), Double.MIN_VALUE);
            }
        }
    }

    public void testOrphanedDocs() throws IOException {
        int numRootDocs = randomIntBetween(1, 20);
        int expectedNestedDocs = 0;
        double expectedSum = 0;
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
                for (int i = 0; i < numRootDocs; i++) {
                    List<Document> documents = new ArrayList<>();
                    int numNestedDocs = randomIntBetween(0, 20);
                    expectedSum += generateSumDocs(documents, numNestedDocs, i, NESTED_OBJECT, VALUE_FIELD_NAME);
                    expectedNestedDocs += numNestedDocs;

                    Document document = new Document();
                    document.add(new Field(UidFieldMapper.NAME, "type#" + i,
                        UidFieldMapper.Defaults.FIELD_TYPE));
                    document.add(new Field(TypeFieldMapper.NAME, "test",
                        TypeFieldMapper.Defaults.FIELD_TYPE));
                    document.add(sequenceIDFields.primaryTerm);
                    documents.add(document);
                    iw.addDocuments(documents);
                }
                //add some random nested docs that don't belong
                List<Document> documents = new ArrayList<>();
                int numOrphanedDocs = randomIntBetween(0, 20);
                generateSumDocs(documents, numOrphanedDocs, 1234, "foo", VALUE_FIELD_NAME);
                iw.addDocuments(documents);
                iw.commit();
            }
            try (IndexReader indexReader = wrap(DirectoryReader.open(directory))) {
                NestedAggregationBuilder nestedBuilder = new NestedAggregationBuilder(NESTED_AGG,
                    NESTED_OBJECT);
                SumAggregationBuilder sumAgg = new SumAggregationBuilder(SUM_AGG_NAME)
                    .field(VALUE_FIELD_NAME);
                nestedBuilder.subAggregation(sumAgg);
                MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType(
                    NumberFieldMapper.NumberType.LONG);
                fieldType.setName(VALUE_FIELD_NAME);

                Nested nested = search(newSearcher(indexReader, false, true),
                    new MatchAllDocsQuery(), nestedBuilder, fieldType);
                assertEquals(expectedNestedDocs, nested.getDocCount());

                assertEquals(NESTED_AGG, nested.getName());
                assertEquals(expectedNestedDocs, nested.getDocCount());

                InternalSum sum = (InternalSum)
                    ((InternalAggregation)nested).getProperty(SUM_AGG_NAME);
                assertEquals(SUM_AGG_NAME, sum.getName());
                assertEquals(expectedSum, sum.getValue(), Double.MIN_VALUE);
            }
        }
    }

    public void testResetRootDocId() throws Exception {
        IndexWriterConfig iwc = new IndexWriterConfig(null);
        iwc.setMergePolicy(NoMergePolicy.INSTANCE);
        SeqNoFieldMapper.SequenceIDFields sequenceIDFields = SeqNoFieldMapper.SequenceIDFields.emptySeqID();
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = new RandomIndexWriter(random(), directory, iwc)) {
                List<Document> documents = new ArrayList<>();

                // 1 segment with, 1 root document, with 3 nested sub docs
                Document document = new Document();
                document.add(new Field(UidFieldMapper.NAME, "type#1", UidFieldMapper.Defaults.NESTED_FIELD_TYPE));
                document.add(new Field(TypeFieldMapper.NAME, "__nested_field", TypeFieldMapper.Defaults.FIELD_TYPE));
                documents.add(document);
                document = new Document();
                document.add(new Field(UidFieldMapper.NAME, "type#1", UidFieldMapper.Defaults.NESTED_FIELD_TYPE));
                document.add(new Field(TypeFieldMapper.NAME, "__nested_field", TypeFieldMapper.Defaults.FIELD_TYPE));
                documents.add(document);
                document = new Document();
                document.add(new Field(UidFieldMapper.NAME, "type#1", UidFieldMapper.Defaults.NESTED_FIELD_TYPE));
                document.add(new Field(TypeFieldMapper.NAME, "__nested_field", TypeFieldMapper.Defaults.FIELD_TYPE));
                documents.add(document);
                document = new Document();
                document.add(new Field(UidFieldMapper.NAME, "type#1", UidFieldMapper.Defaults.FIELD_TYPE));
                document.add(new Field(TypeFieldMapper.NAME, "test", TypeFieldMapper.Defaults.FIELD_TYPE));
                document.add(sequenceIDFields.primaryTerm);
                documents.add(document);
                iw.addDocuments(documents);
                iw.commit();

                documents.clear();
                // 1 segment with:
                // 1 document, with 1 nested subdoc
                document = new Document();
                document.add(new Field(UidFieldMapper.NAME, "type#2", UidFieldMapper.Defaults.NESTED_FIELD_TYPE));
                document.add(new Field(TypeFieldMapper.NAME, "__nested_field", TypeFieldMapper.Defaults.FIELD_TYPE));
                documents.add(document);
                document = new Document();
                document.add(new Field(UidFieldMapper.NAME, "type#2", UidFieldMapper.Defaults.FIELD_TYPE));
                document.add(new Field(TypeFieldMapper.NAME, "test", TypeFieldMapper.Defaults.FIELD_TYPE));
                document.add(sequenceIDFields.primaryTerm);
                documents.add(document);
                iw.addDocuments(documents);
                documents.clear();
                // and 1 document, with 1 nested subdoc
                document = new Document();
                document.add(new Field(UidFieldMapper.NAME, "type#3", UidFieldMapper.Defaults.NESTED_FIELD_TYPE));
                document.add(new Field(TypeFieldMapper.NAME, "__nested_field", TypeFieldMapper.Defaults.FIELD_TYPE));
                documents.add(document);
                document = new Document();
                document.add(new Field(UidFieldMapper.NAME, "type#3", UidFieldMapper.Defaults.FIELD_TYPE));
                document.add(new Field(TypeFieldMapper.NAME, "test", TypeFieldMapper.Defaults.FIELD_TYPE));
                document.add(sequenceIDFields.primaryTerm);
                documents.add(document);
                iw.addDocuments(documents);

                iw.commit();
                iw.close();
            }
            try (IndexReader indexReader = wrap(DirectoryReader.open(directory))) {

                NestedAggregationBuilder nestedBuilder = new NestedAggregationBuilder(NESTED_AGG,
                    "nested_field");
                MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType(
                    NumberFieldMapper.NumberType.LONG);
                fieldType.setName(VALUE_FIELD_NAME);

                BooleanQuery.Builder bq = new BooleanQuery.Builder();
                bq.add(Queries.newNonNestedFilter(VersionUtils.randomVersion(random())), BooleanClause.Occur.MUST);
                bq.add(new TermQuery(new Term(UidFieldMapper.NAME, "type#2")), BooleanClause.Occur.MUST_NOT);

                Nested nested = search(newSearcher(indexReader, false, true),
                    new ConstantScoreQuery(bq.build()), nestedBuilder, fieldType);

                assertEquals(NESTED_AGG, nested.getName());
                // The bug manifests if 6 docs are returned, because currentRootDoc isn't reset the previous child docs from the first segment are emitted as hits.
                assertEquals(4L, nested.getDocCount());
            }
        }
    }

    public void testNestedOrdering() throws IOException {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
                iw.addDocuments(generateBook("1", new String[]{"a"}, new int[]{12, 13, 14}));
                iw.addDocuments(generateBook("2", new String[]{"b"}, new int[]{5, 50}));
                iw.addDocuments(generateBook("3", new String[]{"c"}, new int[]{39, 19}));
                iw.addDocuments(generateBook("4", new String[]{"d"}, new int[]{2, 1, 3}));
                iw.addDocuments(generateBook("5", new String[]{"a"}, new int[]{70, 10}));
                iw.addDocuments(generateBook("6", new String[]{"e"}, new int[]{23, 21}));
                iw.addDocuments(generateBook("7", new String[]{"e", "a"}, new int[]{8, 8}));
                iw.addDocuments(generateBook("8", new String[]{"f"}, new int[]{12, 14}));
                iw.addDocuments(generateBook("9", new String[]{"g", "c", "e"}, new int[]{18, 8}));
            }
            try (IndexReader indexReader = wrap(DirectoryReader.open(directory))) {
                MappedFieldType fieldType1 = new NumberFieldMapper.NumberFieldType(NumberFieldMapper.NumberType.LONG);
                fieldType1.setName("num_pages");
                MappedFieldType fieldType2 = new KeywordFieldMapper.KeywordFieldType();
                fieldType2.setHasDocValues(true);
                fieldType2.setName("author");

                TermsAggregationBuilder termsBuilder = new TermsAggregationBuilder("authors", ValueType.STRING)
                    .field("author").order(BucketOrder.aggregation("chapters>num_pages.value", true));
                NestedAggregationBuilder nestedBuilder = new NestedAggregationBuilder("chapters", "nested_chapters");
                MaxAggregationBuilder maxAgg = new MaxAggregationBuilder("num_pages").field("num_pages");
                nestedBuilder.subAggregation(maxAgg);
                termsBuilder.subAggregation(nestedBuilder);

                Terms terms = search(newSearcher(indexReader, false, true),
                    new MatchAllDocsQuery(), termsBuilder, fieldType1, fieldType2);

                assertEquals(7, terms.getBuckets().size());
                assertEquals("authors", terms.getName());

                Terms.Bucket bucket = terms.getBuckets().get(0);
                assertEquals("d", bucket.getKeyAsString());
                Max numPages = ((Nested) bucket.getAggregations().get("chapters")).getAggregations().get("num_pages");
                assertEquals(3, (int) numPages.getValue());

                bucket = terms.getBuckets().get(1);
                assertEquals("f", bucket.getKeyAsString());
                numPages = ((Nested) bucket.getAggregations().get("chapters")).getAggregations().get("num_pages");
                assertEquals(14, (int) numPages.getValue());

                bucket = terms.getBuckets().get(2);
                assertEquals("g", bucket.getKeyAsString());
                numPages = ((Nested) bucket.getAggregations().get("chapters")).getAggregations().get("num_pages");
                assertEquals(18, (int) numPages.getValue());

                bucket = terms.getBuckets().get(3);
                assertEquals("e", bucket.getKeyAsString());
                numPages = ((Nested) bucket.getAggregations().get("chapters")).getAggregations().get("num_pages");
                assertEquals(23, (int) numPages.getValue());

                bucket = terms.getBuckets().get(4);
                assertEquals("c", bucket.getKeyAsString());
                numPages = ((Nested) bucket.getAggregations().get("chapters")).getAggregations().get("num_pages");
                assertEquals(39, (int) numPages.getValue());

                bucket = terms.getBuckets().get(5);
                assertEquals("b", bucket.getKeyAsString());
                numPages = ((Nested) bucket.getAggregations().get("chapters")).getAggregations().get("num_pages");
                assertEquals(50, (int) numPages.getValue());

                bucket = terms.getBuckets().get(6);
                assertEquals("a", bucket.getKeyAsString());
                numPages = ((Nested) bucket.getAggregations().get("chapters")).getAggregations().get("num_pages");
                assertEquals(70, (int) numPages.getValue());

                // reverse order:
                termsBuilder = new TermsAggregationBuilder("authors", ValueType.STRING)
                    .field("author").order(BucketOrder.aggregation("chapters>num_pages.value", false));
                nestedBuilder = new NestedAggregationBuilder("chapters", "nested_chapters");
                maxAgg = new MaxAggregationBuilder("num_pages").field("num_pages");
                nestedBuilder.subAggregation(maxAgg);
                termsBuilder.subAggregation(nestedBuilder);

                terms = search(newSearcher(indexReader, false, true), new MatchAllDocsQuery(), termsBuilder, fieldType1, fieldType2);

                assertEquals(7, terms.getBuckets().size());
                assertEquals("authors", terms.getName());

                bucket = terms.getBuckets().get(0);
                assertEquals("a", bucket.getKeyAsString());
                numPages = ((Nested) bucket.getAggregations().get("chapters")).getAggregations().get("num_pages");
                assertEquals(70, (int) numPages.getValue());

                bucket = terms.getBuckets().get(1);
                assertEquals("b", bucket.getKeyAsString());
                numPages = ((Nested) bucket.getAggregations().get("chapters")).getAggregations().get("num_pages");
                assertEquals(50, (int) numPages.getValue());

                bucket = terms.getBuckets().get(2);
                assertEquals("c", bucket.getKeyAsString());
                numPages = ((Nested) bucket.getAggregations().get("chapters")).getAggregations().get("num_pages");
                assertEquals(39, (int) numPages.getValue());

                bucket = terms.getBuckets().get(3);
                assertEquals("e", bucket.getKeyAsString());
                numPages = ((Nested) bucket.getAggregations().get("chapters")).getAggregations().get("num_pages");
                assertEquals(23, (int) numPages.getValue());

                bucket = terms.getBuckets().get(4);
                assertEquals("g", bucket.getKeyAsString());
                numPages = ((Nested) bucket.getAggregations().get("chapters")).getAggregations().get("num_pages");
                assertEquals(18, (int) numPages.getValue());

                bucket = terms.getBuckets().get(5);
                assertEquals("f", bucket.getKeyAsString());
                numPages = ((Nested) bucket.getAggregations().get("chapters")).getAggregations().get("num_pages");
                assertEquals(14, (int) numPages.getValue());

                bucket = terms.getBuckets().get(6);
                assertEquals("d", bucket.getKeyAsString());
                numPages = ((Nested) bucket.getAggregations().get("chapters")).getAggregations().get("num_pages");
                assertEquals(3, (int) numPages.getValue());
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
            books.add(Tuple.tuple(String.format(Locale.ROOT, "%03d", i), chapters));
        }
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
                int id = 0;
                for (Tuple<String, int[]> book : books) {
                    iw.addDocuments(generateBook(
                        String.format(Locale.ROOT, "%03d", id), new String[]{book.v1()}, book.v2())
                    );
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
            try (IndexReader indexReader = wrap(DirectoryReader.open(directory))) {
                MappedFieldType fieldType1 = new NumberFieldMapper.NumberFieldType(NumberFieldMapper.NumberType.LONG);
                fieldType1.setName("num_pages");
                MappedFieldType fieldType2 = new KeywordFieldMapper.KeywordFieldType();
                fieldType2.setHasDocValues(true);
                fieldType2.setName("author");

                TermsAggregationBuilder termsBuilder = new TermsAggregationBuilder("authors", ValueType.STRING)
                    .size(books.size()).field("author")
                    .order(BucketOrder.compound(BucketOrder.aggregation("chapters>num_pages.value", true), BucketOrder.key(true)));
                NestedAggregationBuilder nestedBuilder = new NestedAggregationBuilder("chapters", "nested_chapters");
                MinAggregationBuilder minAgg = new MinAggregationBuilder("num_pages").field("num_pages");
                nestedBuilder.subAggregation(minAgg);
                termsBuilder.subAggregation(nestedBuilder);

                Terms terms = search(newSearcher(indexReader, false, true),
                    new MatchAllDocsQuery(), termsBuilder, fieldType1, fieldType2);

                assertEquals(books.size(), terms.getBuckets().size());
                assertEquals("authors", terms.getName());

                for (int i = 0; i < books.size(); i++) {
                    Tuple<String, int[]> book = books.get(i);
                    Terms.Bucket bucket = terms.getBuckets().get(i);
                    assertEquals(book.v1(), bucket.getKeyAsString());
                    Min numPages = ((Nested) bucket.getAggregations().get("chapters")).getAggregations().get("num_pages");
                    assertEquals(book.v2()[0], (int) numPages.getValue());
                }
            }
        }
    }

    public void testPreGetChildLeafCollectors() throws IOException {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
                List<Document> documents = new ArrayList<>();
                Document document = new Document();
                document.add(new Field(UidFieldMapper.NAME, "_doc#1", UidFieldMapper.Defaults.NESTED_FIELD_TYPE));
                document.add(new Field(TypeFieldMapper.NAME, "__nested_field", TypeFieldMapper.Defaults.FIELD_TYPE));
                document.add(new SortedDocValuesField("key", new BytesRef("key1")));
                document.add(new SortedDocValuesField("value", new BytesRef("a1")));
                documents.add(document);
                document = new Document();
                document.add(new Field(UidFieldMapper.NAME, "_doc#1", UidFieldMapper.Defaults.NESTED_FIELD_TYPE));
                document.add(new Field(TypeFieldMapper.NAME, "__nested_field", TypeFieldMapper.Defaults.FIELD_TYPE));
                document.add(new SortedDocValuesField("key", new BytesRef("key2")));
                document.add(new SortedDocValuesField("value", new BytesRef("b1")));
                documents.add(document);
                document = new Document();
                document.add(new Field(UidFieldMapper.NAME, "_doc#1", UidFieldMapper.Defaults.FIELD_TYPE));
                document.add(new Field(TypeFieldMapper.NAME, "_doc", TypeFieldMapper.Defaults.FIELD_TYPE));
                document.add(sequenceIDFields.primaryTerm);
                documents.add(document);
                iw.addDocuments(documents);
                iw.commit();
                documents.clear();

                document = new Document();
                document.add(new Field(UidFieldMapper.NAME, "_doc#2", UidFieldMapper.Defaults.NESTED_FIELD_TYPE));
                document.add(new Field(TypeFieldMapper.NAME, "__nested_field", TypeFieldMapper.Defaults.FIELD_TYPE));
                document.add(new SortedDocValuesField("key", new BytesRef("key1")));
                document.add(new SortedDocValuesField("value", new BytesRef("a2")));
                documents.add(document);
                document = new Document();
                document.add(new Field(UidFieldMapper.NAME, "_doc#2", UidFieldMapper.Defaults.NESTED_FIELD_TYPE));
                document.add(new Field(TypeFieldMapper.NAME, "__nested_field", TypeFieldMapper.Defaults.FIELD_TYPE));
                document.add(new SortedDocValuesField("key", new BytesRef("key2")));
                document.add(new SortedDocValuesField("value", new BytesRef("b2")));
                documents.add(document);
                document = new Document();
                document.add(new Field(UidFieldMapper.NAME, "_doc#2", UidFieldMapper.Defaults.FIELD_TYPE));
                document.add(new Field(TypeFieldMapper.NAME, "_doc", TypeFieldMapper.Defaults.FIELD_TYPE));
                document.add(sequenceIDFields.primaryTerm);
                documents.add(document);
                iw.addDocuments(documents);
                iw.commit();
                documents.clear();

                document = new Document();
                document.add(new Field(UidFieldMapper.NAME, "_doc#3", UidFieldMapper.Defaults.FIELD_TYPE));
                document.add(new Field(TypeFieldMapper.NAME, "__nested_field", TypeFieldMapper.Defaults.FIELD_TYPE));
                document.add(new SortedDocValuesField("key", new BytesRef("key1")));
                document.add(new SortedDocValuesField("value", new BytesRef("a3")));
                documents.add(document);
                document = new Document();
                document.add(new Field(UidFieldMapper.NAME, "_doc#3", UidFieldMapper.Defaults.FIELD_TYPE));
                document.add(new Field(TypeFieldMapper.NAME, "__nested_field", TypeFieldMapper.Defaults.FIELD_TYPE));
                document.add(new SortedDocValuesField("key", new BytesRef("key2")));
                document.add(new SortedDocValuesField("value", new BytesRef("b3")));
                documents.add(document);
                document = new Document();
                document.add(new Field(UidFieldMapper.NAME, "_doc#1", UidFieldMapper.Defaults.FIELD_TYPE));
                document.add(new Field(TypeFieldMapper.NAME, "_doc", TypeFieldMapper.Defaults.FIELD_TYPE));
                document.add(sequenceIDFields.primaryTerm);
                documents.add(document);
                iw.addDocuments(documents);
                iw.commit();
            }
            try (IndexReader indexReader = wrap(DirectoryReader.open(directory))) {
                TermsAggregationBuilder valueBuilder = new TermsAggregationBuilder("value", ValueType.STRING).field("value");
                TermsAggregationBuilder keyBuilder = new TermsAggregationBuilder("key", ValueType.STRING).field("key");
                keyBuilder.subAggregation(valueBuilder);
                NestedAggregationBuilder nestedBuilder = new NestedAggregationBuilder(NESTED_AGG, "nested_field");
                nestedBuilder.subAggregation(keyBuilder);
                FilterAggregationBuilder filterAggregationBuilder = new FilterAggregationBuilder("filterAgg", new MatchAllQueryBuilder());
                filterAggregationBuilder.subAggregation(nestedBuilder);

                MappedFieldType fieldType1 = new KeywordFieldMapper.KeywordFieldType();
                fieldType1.setName("key");
                fieldType1.setHasDocValues(true);
                MappedFieldType fieldType2 = new KeywordFieldMapper.KeywordFieldType();
                fieldType2.setName("value");
                fieldType2.setHasDocValues(true);

                Filter filter = search(newSearcher(indexReader, false, true),
                    Queries.newNonNestedFilter(Version.CURRENT), filterAggregationBuilder, fieldType1, fieldType2);

                assertEquals("filterAgg", filter.getName());
                assertEquals(3L, filter.getDocCount());

                Nested nested = filter.getAggregations().get(NESTED_AGG);
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

    private double generateMaxDocs(List<Document> documents, int numNestedDocs, int id, String path, String fieldName) {
        return DoubleStream.of(generateDocuments(documents, numNestedDocs, id, path, fieldName))
            .max().orElse(Double.NEGATIVE_INFINITY);
    }

    private double generateSumDocs(List<Document> documents, int numNestedDocs, int id, String path, String fieldName) {
        return DoubleStream.of(generateDocuments(documents, numNestedDocs, id, path, fieldName)).sum();
    }

    private double[] generateDocuments(List<Document> documents, int numNestedDocs, int id, String path, String fieldName) {

        double[] values = new double[numNestedDocs];
        for (int nested = 0; nested < numNestedDocs; nested++) {
            Document document = new Document();
            document.add(new Field(UidFieldMapper.NAME, "type#" + id,
                UidFieldMapper.Defaults.NESTED_FIELD_TYPE));
            document.add(new Field(TypeFieldMapper.NAME, "__" + path,
                TypeFieldMapper.Defaults.FIELD_TYPE));
            long value = randomNonNegativeLong() % 10000;
            document.add(new SortedNumericDocValuesField(fieldName, value));
            documents.add(document);
            values[nested] = value;
        }
        return values;
    }

    private List<Document> generateBook(String id, String[] authors, int[] numPages) {
        List<Document> documents = new ArrayList<>();

        for (int numPage : numPages) {
            Document document = new Document();
            document.add(new Field(UidFieldMapper.NAME, "book#" + id, UidFieldMapper.Defaults.NESTED_FIELD_TYPE));
            document.add(new Field(TypeFieldMapper.NAME, "__nested_chapters", TypeFieldMapper.Defaults.FIELD_TYPE));
            document.add(new SortedNumericDocValuesField("num_pages", numPage));
            documents.add(document);
        }

        Document document = new Document();
        document.add(new Field(UidFieldMapper.NAME, "book#" + id, UidFieldMapper.Defaults.FIELD_TYPE));
        document.add(new Field(TypeFieldMapper.NAME, "book", TypeFieldMapper.Defaults.FIELD_TYPE));
        document.add(sequenceIDFields.primaryTerm);
        for (String author : authors) {
            document.add(new SortedSetDocValuesField("author", new BytesRef(author)));
        }
        documents.add(document);

        return documents;
    }

}
