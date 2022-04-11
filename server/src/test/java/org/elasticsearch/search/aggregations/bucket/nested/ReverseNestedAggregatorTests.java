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
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NestedPathFieldMapper;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.mapper.ObjectMapper;
import org.elasticsearch.index.mapper.ProvidedIdFieldMapper;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.bucket.terms.LongTerms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.Max;
import org.elasticsearch.search.aggregations.metrics.MaxAggregationBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.LongStream;

import static java.util.stream.Collectors.toList;
import static org.elasticsearch.search.aggregations.AggregationBuilders.max;
import static org.elasticsearch.search.aggregations.AggregationBuilders.nested;
import static org.elasticsearch.search.aggregations.AggregationBuilders.reverseNested;
import static org.hamcrest.Matchers.equalTo;

public class ReverseNestedAggregatorTests extends AggregatorTestCase {

    private static final String VALUE_FIELD_NAME = "number";
    private static final String NESTED_OBJECT = "nested_object";
    private static final String NESTED_AGG = "nestedAgg";
    private static final String REVERSE_AGG_NAME = "reverseNestedAgg";
    private static final String MAX_AGG_NAME = "maxAgg";

    /**
     * Nested aggregations need the {@linkplain DirectoryReader} wrapped.
     */
    @Override
    protected IndexReader wrapDirectoryReader(DirectoryReader reader) throws IOException {
        return wrapInMockESDirectoryReader(reader);
    }

    public void testNoDocs() throws IOException {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
                // intentionally not writing any docs
            }
            try (IndexReader indexReader = wrapInMockESDirectoryReader(DirectoryReader.open(directory))) {
                NestedAggregationBuilder nestedBuilder = new NestedAggregationBuilder(NESTED_AGG, NESTED_OBJECT);
                ReverseNestedAggregationBuilder reverseNestedBuilder = new ReverseNestedAggregationBuilder(REVERSE_AGG_NAME);
                nestedBuilder.subAggregation(reverseNestedBuilder);
                MaxAggregationBuilder maxAgg = new MaxAggregationBuilder(MAX_AGG_NAME).field(VALUE_FIELD_NAME);
                reverseNestedBuilder.subAggregation(maxAgg);
                MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType(VALUE_FIELD_NAME, NumberFieldMapper.NumberType.LONG);

                Nested nested = searchAndReduce(newSearcher(indexReader, false, true), new MatchAllDocsQuery(), nestedBuilder, fieldType);
                ReverseNested reverseNested = (ReverseNested) ((InternalAggregation) nested).getProperty(REVERSE_AGG_NAME);
                assertEquals(REVERSE_AGG_NAME, reverseNested.getName());
                assertEquals(0, reverseNested.getDocCount());

                Max max = (Max) ((InternalAggregation) reverseNested).getProperty(MAX_AGG_NAME);
                assertEquals(MAX_AGG_NAME, max.getName());
                assertEquals(Double.NEGATIVE_INFINITY, max.value(), Double.MIN_VALUE);
            }
        }
    }

    public void testMaxFromParentDocs() throws IOException {
        int numParentDocs = randomIntBetween(1, 20);
        int expectedParentDocs = 0;
        int expectedNestedDocs = 0;
        double expectedMaxValue = Double.NEGATIVE_INFINITY;
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
                for (int i = 0; i < numParentDocs; i++) {
                    List<Document> documents = new ArrayList<>();
                    int numNestedDocs = randomIntBetween(0, 20);
                    for (int nested = 0; nested < numNestedDocs; nested++) {
                        Document document = new Document();
                        document.add(
                            new Field(
                                IdFieldMapper.NAME,
                                Uid.encodeId(Integer.toString(i)),
                                ProvidedIdFieldMapper.Defaults.NESTED_FIELD_TYPE
                            )
                        );
                        document.add(new Field(NestedPathFieldMapper.NAME, NESTED_OBJECT, NestedPathFieldMapper.Defaults.FIELD_TYPE));
                        documents.add(document);
                        expectedNestedDocs++;
                    }
                    Document document = new Document();
                    document.add(
                        new Field(IdFieldMapper.NAME, Uid.encodeId(Integer.toString(i)), ProvidedIdFieldMapper.Defaults.FIELD_TYPE)
                    );
                    document.add(new Field(NestedPathFieldMapper.NAME, "test", NestedPathFieldMapper.Defaults.FIELD_TYPE));
                    long value = randomNonNegativeLong() % 10000;
                    document.add(new SortedNumericDocValuesField(VALUE_FIELD_NAME, value));
                    document.add(SeqNoFieldMapper.SequenceIDFields.emptySeqID().primaryTerm);
                    if (numNestedDocs > 0) {
                        expectedMaxValue = Math.max(expectedMaxValue, value);
                        expectedParentDocs++;
                    }
                    documents.add(document);
                    iw.addDocuments(documents);
                }
                iw.commit();
            }
            try (IndexReader indexReader = wrapInMockESDirectoryReader(DirectoryReader.open(directory))) {
                NestedAggregationBuilder nestedBuilder = new NestedAggregationBuilder(NESTED_AGG, NESTED_OBJECT);
                ReverseNestedAggregationBuilder reverseNestedBuilder = new ReverseNestedAggregationBuilder(REVERSE_AGG_NAME);
                nestedBuilder.subAggregation(reverseNestedBuilder);
                MaxAggregationBuilder maxAgg = new MaxAggregationBuilder(MAX_AGG_NAME).field(VALUE_FIELD_NAME);
                reverseNestedBuilder.subAggregation(maxAgg);
                MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType(VALUE_FIELD_NAME, NumberFieldMapper.NumberType.LONG);

                Nested nested = searchAndReduce(newSearcher(indexReader, false, true), new MatchAllDocsQuery(), nestedBuilder, fieldType);
                assertEquals(expectedNestedDocs, nested.getDocCount());

                ReverseNested reverseNested = (ReverseNested) ((InternalAggregation) nested).getProperty(REVERSE_AGG_NAME);
                assertEquals(REVERSE_AGG_NAME, reverseNested.getName());
                assertEquals(expectedParentDocs, reverseNested.getDocCount());

                Max max = (Max) ((InternalAggregation) reverseNested).getProperty(MAX_AGG_NAME);
                assertEquals(MAX_AGG_NAME, max.getName());
                assertEquals(expectedMaxValue, max.value(), Double.MIN_VALUE);
            }
        }
    }

    public void testFieldAlias() throws IOException {
        int numParentDocs = randomIntBetween(1, 20);
        int expectedParentDocs = 0;

        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType(VALUE_FIELD_NAME, NumberFieldMapper.NumberType.LONG);

        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
                for (int i = 0; i < numParentDocs; i++) {
                    List<Document> documents = new ArrayList<>();
                    int numNestedDocs = randomIntBetween(0, 20);
                    if (numNestedDocs > 0) {
                        expectedParentDocs++;
                    }

                    for (int nested = 0; nested < numNestedDocs; nested++) {
                        Document document = new Document();
                        document.add(
                            new Field(
                                IdFieldMapper.NAME,
                                Uid.encodeId(Integer.toString(i)),
                                ProvidedIdFieldMapper.Defaults.NESTED_FIELD_TYPE
                            )
                        );
                        document.add(new Field(NestedPathFieldMapper.NAME, NESTED_OBJECT, NestedPathFieldMapper.Defaults.FIELD_TYPE));
                        documents.add(document);
                    }
                    Document document = new Document();
                    document.add(
                        new Field(IdFieldMapper.NAME, Uid.encodeId(Integer.toString(i)), ProvidedIdFieldMapper.Defaults.FIELD_TYPE)
                    );
                    document.add(new Field(NestedPathFieldMapper.NAME, "test", NestedPathFieldMapper.Defaults.FIELD_TYPE));

                    long value = randomNonNegativeLong() % 10000;
                    document.add(new SortedNumericDocValuesField(VALUE_FIELD_NAME, value));
                    document.add(SeqNoFieldMapper.SequenceIDFields.emptySeqID().primaryTerm);
                    documents.add(document);
                    iw.addDocuments(documents);
                }
                iw.commit();
            }

            try (IndexReader indexReader = wrapInMockESDirectoryReader(DirectoryReader.open(directory))) {
                MaxAggregationBuilder maxAgg = max(MAX_AGG_NAME).field(VALUE_FIELD_NAME);
                MaxAggregationBuilder aliasMaxAgg = max(MAX_AGG_NAME).field(VALUE_FIELD_NAME + "-alias");

                NestedAggregationBuilder agg = nested(NESTED_AGG, NESTED_OBJECT).subAggregation(
                    reverseNested(REVERSE_AGG_NAME).subAggregation(maxAgg)
                );
                NestedAggregationBuilder aliasAgg = nested(NESTED_AGG, NESTED_OBJECT).subAggregation(
                    reverseNested(REVERSE_AGG_NAME).subAggregation(aliasMaxAgg)
                );

                Nested nested = searchAndReduce(newSearcher(indexReader, false, true), new MatchAllDocsQuery(), agg, fieldType);
                Nested aliasNested = searchAndReduce(newSearcher(indexReader, false, true), new MatchAllDocsQuery(), aliasAgg, fieldType);

                ReverseNested reverseNested = nested.getAggregations().get(REVERSE_AGG_NAME);
                ReverseNested aliasReverseNested = aliasNested.getAggregations().get(REVERSE_AGG_NAME);

                assertEquals(reverseNested, aliasReverseNested);
                assertEquals(expectedParentDocs, reverseNested.getDocCount());
            }
        }
    }

    public void testNestedUnderTerms() throws IOException {
        int numProducts = scaledRandomIntBetween(1, 100);
        int numResellers = scaledRandomIntBetween(1, 100);

        AggregationBuilder b = new NestedAggregationBuilder("nested", "nested_reseller").subAggregation(
            new TermsAggregationBuilder("resellers").field("reseller_id")
                .size(numResellers)
                .subAggregation(
                    new ReverseNestedAggregationBuilder("reverse_nested").subAggregation(
                        new TermsAggregationBuilder("products").field("product_id").size(numProducts)
                    )
                )
        );
        testCase(b, new MatchAllDocsQuery(), NestedAggregatorTests.buildResellerData(numProducts, numResellers), result -> {
            InternalNested nested = (InternalNested) result;
            assertThat(nested.getDocCount(), equalTo((long) numProducts * numResellers));
            LongTerms resellers = nested.getAggregations().get("resellers");
            assertThat(
                resellers.getBuckets().stream().map(LongTerms.Bucket::getKeyAsNumber).collect(toList()),
                equalTo(LongStream.range(0, numResellers).mapToObj(Long::valueOf).collect(toList()))
            );
            for (int r = 0; r < numResellers; r++) {
                LongTerms.Bucket bucket = resellers.getBucketByKey(Integer.toString(r));
                assertThat(bucket.getDocCount(), equalTo((long) numProducts));
                InternalReverseNested reverseNested = bucket.getAggregations().get("reverse_nested");
                assertThat(reverseNested.getDocCount(), equalTo((long) numProducts));
                LongTerms products = reverseNested.getAggregations().get("products");
                assertThat(
                    products.getBuckets().stream().map(LongTerms.Bucket::getKeyAsNumber).collect(toList()),
                    equalTo(LongStream.range(0, numProducts).mapToObj(Long::valueOf).collect(toList()))
                );
            }
        }, NestedAggregatorTests.resellersMappedFields());
    }

    @Override
    protected List<ObjectMapper> objectMappers() {
        return NestedAggregatorTests.MOCK_OBJECT_MAPPERS;
    }
}
