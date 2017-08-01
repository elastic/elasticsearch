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
import org.apache.lucene.document.SortedNumericDocValuesField;
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
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.mapper.TypeFieldMapper;
import org.elasticsearch.index.mapper.UidFieldMapper;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.max.InternalMax;
import org.elasticsearch.search.aggregations.metrics.max.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.sum.InternalSum;
import org.elasticsearch.search.aggregations.metrics.sum.SumAggregationBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.DoubleStream;

public class NestedAggregatorTests extends AggregatorTestCase {

    private static final String VALUE_FIELD_NAME = "number";
    private static final String NESTED_OBJECT = "nested_object";
    private static final String NESTED_OBJECT2 = "nested_object2";
    private static final String NESTED_AGG = "nestedAgg";
    private static final String MAX_AGG_NAME = "maxAgg";
    private static final String SUM_AGG_NAME = "sumAgg";

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
                bq.add(Queries.newNonNestedFilter(), BooleanClause.Occur.MUST);
                bq.add(new TermQuery(new Term(UidFieldMapper.NAME, "type#2")), BooleanClause.Occur.MUST_NOT);

                Nested nested = search(newSearcher(indexReader, false, true),
                    new ConstantScoreQuery(bq.build()), nestedBuilder, fieldType);

                assertEquals(NESTED_AGG, nested.getName());
                // The bug manifests if 6 docs are returned, because currentRootDoc isn't reset the previous child docs from the first segment are emitted as hits.
                assertEquals(4L, nested.getDocCount());
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

}
