/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.join.aggregations;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.join.ParentJoinPlugin;
import org.elasticsearch.join.mapper.ParentIdFieldMapper;
import org.elasticsearch.join.mapper.ParentJoinFieldMapper;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.bucket.terms.LongTerms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.Min;
import org.elasticsearch.search.aggregations.metrics.MinAggregationBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Consumer;

public class ChildrenToParentAggregatorTests extends AggregatorTestCase {

    private static final String CHILD_TYPE = "child_type";
    private static final String PARENT_TYPE = "parent_type";

    public void testNoDocs() throws IOException {
        Directory directory = newDirectory();

        RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);
        // intentionally not writing any docs
        indexWriter.close();
        DirectoryReader indexReader = DirectoryReader.open(directory);

        testCase(new MatchAllDocsQuery(), indexReader, childrenToParent -> {
            assertEquals(0, childrenToParent.getDocCount());
            Aggregation parentAggregation = childrenToParent.getAggregations().get("in_parent");
            assertEquals(0, childrenToParent.getDocCount());
            assertNotNull("Aggregations: " + childrenToParent.getAggregations().asList(), parentAggregation);
            assertEquals(Double.POSITIVE_INFINITY, ((Min) parentAggregation).value(), Double.MIN_VALUE);
            assertFalse(JoinAggregationInspectionHelper.hasValue(childrenToParent));
        });
        indexReader.close();
        directory.close();
    }

    public void testParentChild() throws IOException {
        Directory directory = newDirectory();
        RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);

        final Map<String, Tuple<Integer, Integer>> expectedParentChildRelations = setupIndex(indexWriter);
        indexWriter.close();

        DirectoryReader indexReader = ElasticsearchDirectoryReader.wrap(
            DirectoryReader.open(directory),
            new ShardId(new Index("foo", "_na_"), 1)
        );
        // verify with all documents
        testCase(new MatchAllDocsQuery(), indexReader, parent -> {
            int expectedTotalParents = 0;
            int expectedMinValue = Integer.MAX_VALUE;
            for (Tuple<Integer, Integer> expectedValues : expectedParentChildRelations.values()) {
                expectedTotalParents++;
                expectedMinValue = Math.min(expectedMinValue, expectedValues.v2());
            }
            assertEquals(
                "Having " + parent.getDocCount() + " docs and aggregation results: " + parent,
                expectedTotalParents,
                parent.getDocCount()
            );
            assertEquals(expectedMinValue, ((Min) parent.getAggregations().get("in_parent")).value(), Double.MIN_VALUE);
            assertTrue(JoinAggregationInspectionHelper.hasValue(parent));
        });

        // verify for each children
        for (String parent : expectedParentChildRelations.keySet()) {
            testCase(new TermInSetQuery(IdFieldMapper.NAME, List.of(Uid.encodeId("child0_" + parent))), indexReader, aggregation -> {
                assertEquals(
                    "Expected one result for min-aggregation for parent: " + parent + ", but had aggregation-results: " + aggregation,
                    1,
                    aggregation.getDocCount()
                );
                assertEquals(
                    expectedParentChildRelations.get(parent).v2(),
                    ((Min) aggregation.getAggregations().get("in_parent")).value(),
                    Double.MIN_VALUE
                );
            });
        }

        indexReader.close();
        directory.close();
    }

    public void testParentChildTerms() throws IOException {
        Directory directory = newDirectory();
        RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);

        final Map<String, Tuple<Integer, Integer>> expectedParentChildRelations = setupIndex(indexWriter);
        indexWriter.close();

        SortedMap<Integer, Long> entries = new TreeMap<>();
        for (Tuple<Integer, Integer> value : expectedParentChildRelations.values()) {
            Long l = entries.computeIfAbsent(value.v2(), integer -> 0L);
            entries.put(value.v2(), l + 1);
        }
        List<Map.Entry<Integer, Long>> sortedValues = new ArrayList<>(entries.entrySet());
        sortedValues.sort((o1, o2) -> {
            // sort larger values first
            int ret = o2.getValue().compareTo(o1.getValue());
            if (ret != 0) {
                return ret;
            }

            // on equal value, sort by key
            return o1.getKey().compareTo(o2.getKey());
        });

        DirectoryReader indexReader = ElasticsearchDirectoryReader.wrap(
            DirectoryReader.open(directory),
            new ShardId(new Index("foo", "_na_"), 1)
        );
        // verify a terms-aggregation inside the parent-aggregation
        testCaseTerms(new MatchAllDocsQuery(), indexReader, parent -> {
            assertNotNull(parent);
            assertTrue(JoinAggregationInspectionHelper.hasValue(parent));
            LongTerms valueTerms = parent.getAggregations().get("value_terms");
            assertNotNull(valueTerms);

            List<LongTerms.Bucket> valueTermsBuckets = valueTerms.getBuckets();
            assertNotNull(valueTermsBuckets);
            assertEquals("Had: " + parent, sortedValues.size(), valueTermsBuckets.size());
            int i = 0;
            for (Map.Entry<Integer, Long> entry : sortedValues) {
                LongTerms.Bucket bucket = valueTermsBuckets.get(i);
                assertEquals(entry.getKey().longValue(), bucket.getKeyAsNumber());
                assertEquals(entry.getValue(), (Long) bucket.getDocCount());

                i++;
            }
        });

        indexReader.close();
        directory.close();
    }

    public void testTermsParentChildTerms() throws IOException {
        Directory directory = newDirectory();
        RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);

        final Map<String, Tuple<Integer, Integer>> expectedParentChildRelations = setupIndex(indexWriter);
        indexWriter.close();

        SortedMap<Integer, Long> sortedValues = new TreeMap<>();
        for (Tuple<Integer, Integer> value : expectedParentChildRelations.values()) {
            Long l = sortedValues.computeIfAbsent(value.v2(), integer -> 0L);
            sortedValues.put(value.v2(), l + 1);
        }

        DirectoryReader indexReader = ElasticsearchDirectoryReader.wrap(
            DirectoryReader.open(directory),
            new ShardId(new Index("foo", "_na_"), 1)
        );
        // verify a terms-aggregation inside the parent-aggregation which itself is inside a
        // terms-aggregation on the child-documents
        testCaseTermsParentTerms(new MatchAllDocsQuery(), indexReader, longTerms -> {
            assertNotNull(longTerms);

            for (LongTerms.Bucket bucket : longTerms.getBuckets()) {
                assertNotNull(bucket);
                assertNotNull(bucket.getKeyAsString());
            }
        });

        indexReader.close();
        directory.close();
    }

    private static Map<String, Tuple<Integer, Integer>> setupIndex(RandomIndexWriter iw) throws IOException {
        Map<String, Tuple<Integer, Integer>> expectedValues = new HashMap<>();
        int numParents = randomIntBetween(1, 10);
        for (int i = 0; i < numParents; i++) {
            List<List<Field>> documents = new ArrayList<>();
            String parent = "parent" + i;
            int randomValue = randomIntBetween(0, 100);
            documents.add(createParentDocument(parent, randomValue));
            int numChildren = randomIntBetween(1, 10);
            int minValue = Integer.MAX_VALUE;
            for (int c = 0; c < numChildren; c++) {
                minValue = Math.min(minValue, randomValue);
                int randomSubValue = randomIntBetween(0, 100);
                documents.add(createChildDocument("child" + c + "_" + parent, parent, randomSubValue));
            }
            expectedValues.put(parent, new Tuple<>(numChildren, minValue));
            iw.addDocuments(documents);
        }
        return expectedValues;
    }

    private static List<Field> createParentDocument(String id, int value) {
        return Arrays.asList(
            new StringField(IdFieldMapper.NAME, Uid.encodeId(id), Field.Store.NO),
            new StringField("join_field", PARENT_TYPE, Field.Store.NO),
            createJoinField(PARENT_TYPE, id),
            new SortedNumericDocValuesField("number", value)
        );
    }

    private static List<Field> createChildDocument(String childId, String parentId, int value) {
        return Arrays.asList(
            new StringField(IdFieldMapper.NAME, Uid.encodeId(childId), Field.Store.NO),
            new StringField("join_field", CHILD_TYPE, Field.Store.NO),
            createJoinField(PARENT_TYPE, parentId),
            new SortedNumericDocValuesField("subNumber", value)
        );
    }

    private static SortedDocValuesField createJoinField(String parentType, String id) {
        return new SortedDocValuesField("join_field#" + parentType, new BytesRef(id));
    }

    private void testCase(Query query, DirectoryReader reader, Consumer<InternalParent> verify) throws IOException {

        ParentAggregationBuilder aggregationBuilder = new ParentAggregationBuilder("_name", CHILD_TYPE);
        aggregationBuilder.subAggregation(new MinAggregationBuilder("in_parent").field("number"));

        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("number", NumberFieldMapper.NumberType.LONG);
        InternalParent result = searchAndReduce(reader, new AggTestConfig(aggregationBuilder, withJoinFields(fieldType)).withQuery(query));
        verify.accept(result);
    }

    private void testCaseTerms(Query query, DirectoryReader reader, Consumer<InternalParent> verify) throws IOException {

        ParentAggregationBuilder aggregationBuilder = new ParentAggregationBuilder("_name", CHILD_TYPE);
        aggregationBuilder.subAggregation(new TermsAggregationBuilder("value_terms").field("number"));

        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("number", NumberFieldMapper.NumberType.LONG);
        InternalParent result = searchAndReduce(reader, new AggTestConfig(aggregationBuilder, withJoinFields(fieldType)).withQuery(query));
        verify.accept(result);
    }

    // run a terms aggregation on the number in child-documents, then a parent aggregation and then terms on the parent-number
    private void testCaseTermsParentTerms(Query query, DirectoryReader reader, Consumer<LongTerms> verify) throws IOException {
        AggregationBuilder aggregationBuilder = new TermsAggregationBuilder("subvalue_terms").field("subNumber")
            .subAggregation(
                new ParentAggregationBuilder("to_parent", CHILD_TYPE).subAggregation(
                    new TermsAggregationBuilder("value_terms").field("number")
                )
            );

        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("number", NumberFieldMapper.NumberType.LONG);
        MappedFieldType subFieldType = new NumberFieldMapper.NumberFieldType("subNumber", NumberFieldMapper.NumberType.LONG);
        LongTerms result = searchAndReduce(
            reader,
            new AggTestConfig(aggregationBuilder, withJoinFields(fieldType, subFieldType)).withQuery(query)
        );
        verify.accept(result);
    }

    @Override
    protected List<SearchPlugin> getSearchPlugins() {
        return Collections.singletonList(new ParentJoinPlugin());
    }

    static MappedFieldType[] withJoinFields(MappedFieldType... fieldTypes) {
        MappedFieldType[] result = new MappedFieldType[fieldTypes.length + 2];
        System.arraycopy(fieldTypes, 0, result, 0, fieldTypes.length);

        int i = fieldTypes.length;
        result[i++] = new ParentJoinFieldMapper.Builder("join_field").addRelation(PARENT_TYPE, Collections.singleton(CHILD_TYPE))
            .build(MapperBuilderContext.root(false, false))
            .fieldType();
        result[i++] = new ParentIdFieldMapper.ParentIdFieldType("join_field#" + PARENT_TYPE, false);
        assert i == result.length;
        return result;
    }
}
