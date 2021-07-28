/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.join.aggregations;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.join.ParentJoinPlugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.InternalMin;
import org.elasticsearch.search.aggregations.metrics.MinAggregationBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.elasticsearch.join.aggregations.ChildrenToParentAggregatorTests.withJoinFields;
import static org.hamcrest.Matchers.equalTo;

public class ParentToChildrenAggregatorTests extends AggregatorTestCase {

    private static final String CHILD_TYPE = "child_type";
    private static final String PARENT_TYPE = "parent_type";

    public void testNoDocs() throws IOException {
        Directory directory = newDirectory();

        RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);
        // intentionally not writing any docs
        indexWriter.close();
        IndexReader indexReader = DirectoryReader.open(directory);

        testCase(new MatchAllDocsQuery(), newSearcher(indexReader, false, true), parentToChild -> {
            assertEquals(0, parentToChild.getDocCount());
            assertEquals(Double.POSITIVE_INFINITY, ((InternalMin) parentToChild.getAggregations().get("in_child")).getValue(),
                    Double.MIN_VALUE);
        });
        indexReader.close();
        directory.close();
    }

    public void testParentChild() throws IOException {
        Directory directory = newDirectory();
        RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);

        final Map<String, Tuple<Integer, Integer>> expectedParentChildRelations = setupIndex(indexWriter);
        indexWriter.close();

        IndexReader indexReader = ElasticsearchDirectoryReader.wrap(DirectoryReader.open(directory),
                new ShardId(new Index("foo", "_na_"), 1));
        // TODO set "maybeWrap" to true for IndexSearcher once #23338 is resolved
        IndexSearcher indexSearcher = newSearcher(indexReader, false, true);

        testCase(new MatchAllDocsQuery(), indexSearcher, child -> {
            int expectedTotalChildren = 0;
            int expectedMinValue = Integer.MAX_VALUE;
            for (Tuple<Integer, Integer> expectedValues : expectedParentChildRelations.values()) {
                expectedTotalChildren += expectedValues.v1();
                expectedMinValue = Math.min(expectedMinValue, expectedValues.v2());
            }
            assertEquals(expectedTotalChildren, child.getDocCount());
            assertTrue(JoinAggregationInspectionHelper.hasValue(child));
            assertEquals(expectedMinValue, ((InternalMin) child.getAggregations().get("in_child")).getValue(), Double.MIN_VALUE);
        });

        for (String parent : expectedParentChildRelations.keySet()) {
            testCase(new TermInSetQuery(IdFieldMapper.NAME, Uid.encodeId(parent)), indexSearcher, child -> {
                assertEquals((long) expectedParentChildRelations.get(parent).v1(), child.getDocCount());
                assertEquals(expectedParentChildRelations.get(parent).v2(),
                        ((InternalMin) child.getAggregations().get("in_child")).getValue(), Double.MIN_VALUE);
            });
        }
        indexReader.close();
        directory.close();
    }

    public void testParentChildAsSubAgg() throws IOException {
        MappedFieldType kwd = new KeywordFieldMapper.KeywordFieldType("kwd", randomBoolean(), true, Collections.emptyMap());
        try (Directory directory = newDirectory()) {
            RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);

            final Map<String, Tuple<Integer, Integer>> expectedParentChildRelations = setupIndex(indexWriter);
            indexWriter.close();

            try (
                IndexReader indexReader = ElasticsearchDirectoryReader.wrap(
                    DirectoryReader.open(directory),
                    new ShardId(new Index("foo", "_na_"), 1)
                )
            ) {
                IndexSearcher indexSearcher = newSearcher(indexReader, false, true);

                AggregationBuilder request = new TermsAggregationBuilder("t").field("kwd")
                    .subAggregation(
                        new ChildrenAggregationBuilder("children", CHILD_TYPE).subAggregation(
                            new MinAggregationBuilder("min").field("number")
                        )
                    );

                long expectedEvenChildCount = 0;
                double expectedEvenMin = Double.MAX_VALUE;
                long expectedOddChildCount = 0;
                double expectedOddMin = Double.MAX_VALUE;
                for (Map.Entry<String, Tuple<Integer, Integer>> e : expectedParentChildRelations.entrySet()) {
                    if (Integer.valueOf(e.getKey().substring("parent".length())) % 2 == 0) {
                        expectedEvenChildCount += e.getValue().v1();
                        expectedEvenMin = Math.min(expectedEvenMin, e.getValue().v2());
                    } else {
                        expectedOddChildCount += e.getValue().v1();
                        expectedOddMin = Math.min(expectedOddMin, e.getValue().v2());
                    }
                }
                StringTerms result = searchAndReduce(
                    indexSearcher,
                    new MatchAllDocsQuery(),
                    request,
                    withJoinFields(longField("number"), kwd)
                );

                StringTerms.Bucket evenBucket = result.getBucketByKey("even");
                InternalChildren evenChildren = evenBucket.getAggregations().get("children");
                InternalMin evenMin = evenChildren.getAggregations().get("min");
                assertThat(evenChildren.getDocCount(), equalTo(expectedEvenChildCount));
                assertThat(evenMin.getValue(), equalTo(expectedEvenMin));

                if (expectedOddChildCount > 0) {
                    StringTerms.Bucket oddBucket = result.getBucketByKey("odd");
                    InternalChildren oddChildren = oddBucket.getAggregations().get("children");
                    InternalMin oddMin = oddChildren.getAggregations().get("min");
                    assertThat(oddChildren.getDocCount(), equalTo(expectedOddChildCount));
                    assertThat(oddMin.getValue(), equalTo(expectedOddMin));
                } else {
                    assertNull(result.getBucketByKey("odd"));
                }
            }
        }
    }

    private static Map<String, Tuple<Integer, Integer>> setupIndex(RandomIndexWriter iw) throws IOException {
        Map<String, Tuple<Integer, Integer>> expectedValues = new HashMap<>();
        int numParents = randomIntBetween(1, 10);
        for (int i = 0; i < numParents; i++) {
            String parent = "parent" + i;
            iw.addDocument(createParentDocument(parent, i % 2 == 0 ? "even" : "odd"));
            int numChildren = randomIntBetween(1, 10);
            int minValue = Integer.MAX_VALUE;
            for (int c = 0; c < numChildren; c++) {
                int randomValue = randomIntBetween(0, 100);
                minValue = Math.min(minValue, randomValue);
                iw.addDocument(createChildDocument("child" + c + "_" + parent, parent, randomValue));
            }
            expectedValues.put(parent, new Tuple<>(numChildren, minValue));
        }
        return expectedValues;
    }

    private static List<Field> createParentDocument(String id, String kwd) {
        return Arrays.asList(
                new StringField(IdFieldMapper.NAME, Uid.encodeId(id), Field.Store.NO),
                new SortedSetDocValuesField("kwd", new BytesRef(kwd)),
                new Field("kwd", new BytesRef(kwd), KeywordFieldMapper.Defaults.FIELD_TYPE),
                new StringField("join_field", PARENT_TYPE, Field.Store.NO),
                createJoinField(PARENT_TYPE, id)
        );
    }

    private static List<Field> createChildDocument(String childId, String parentId, int value) {
        return Arrays.asList(
                new StringField(IdFieldMapper.NAME, Uid.encodeId(childId), Field.Store.NO),
                new StringField("join_field", CHILD_TYPE, Field.Store.NO),
                createJoinField(PARENT_TYPE, parentId),
                new SortedNumericDocValuesField("number", value)
        );
    }

    private static SortedDocValuesField createJoinField(String parentType, String id) {
        return new SortedDocValuesField("join_field#" + parentType, new BytesRef(id));
    }

    private void testCase(Query query, IndexSearcher indexSearcher, Consumer<InternalChildren> verify)
            throws IOException {

        ChildrenAggregationBuilder aggregationBuilder = new ChildrenAggregationBuilder("_name", CHILD_TYPE);
        aggregationBuilder.subAggregation(new MinAggregationBuilder("in_child").field("number"));

        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("number", NumberFieldMapper.NumberType.LONG);
        InternalChildren result = searchAndReduce(indexSearcher, query, aggregationBuilder, withJoinFields(fieldType));
        verify.accept(result);
    }

    @Override
    protected List<SearchPlugin> getSearchPlugins() {
        return Collections.singletonList(new ParentJoinPlugin());
    }
}
