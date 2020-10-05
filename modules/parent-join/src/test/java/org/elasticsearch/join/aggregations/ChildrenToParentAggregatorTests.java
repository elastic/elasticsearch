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

package org.elasticsearch.join.aggregations;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
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
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.ContentPath;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.join.ParentJoinPlugin;
import org.elasticsearch.join.mapper.MetaJoinFieldMapper;
import org.elasticsearch.join.mapper.ParentJoinFieldMapper;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.bucket.terms.LongTerms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.InternalMin;
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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ChildrenToParentAggregatorTests extends AggregatorTestCase {

    private static final String CHILD_TYPE = "child_type";
    private static final String PARENT_TYPE = "parent_type";

    public void testNoDocs() throws IOException {
        Directory directory = newDirectory();

        RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);
        // intentionally not writing any docs
        indexWriter.close();
        IndexReader indexReader = DirectoryReader.open(directory);

        testCase(new MatchAllDocsQuery(), newSearcher(indexReader, false, true), childrenToParent -> {
            assertEquals(0, childrenToParent.getDocCount());
            Aggregation parentAggregation = childrenToParent.getAggregations().get("in_parent");
            assertEquals(0, childrenToParent.getDocCount());
            assertNotNull("Aggregations: " + childrenToParent.getAggregations().asMap(), parentAggregation);
            assertEquals(Double.POSITIVE_INFINITY, ((InternalMin) parentAggregation).getValue(), Double.MIN_VALUE);
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

        IndexReader indexReader = ElasticsearchDirectoryReader.wrap(DirectoryReader.open(directory),
                new ShardId(new Index("foo", "_na_"), 1));
        // TODO set "maybeWrap" to true for IndexSearcher once #23338 is resolved
        IndexSearcher indexSearcher = newSearcher(indexReader, false, true);

        // verify with all documents
        testCase(new MatchAllDocsQuery(), indexSearcher, parent -> {
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
            assertEquals(expectedMinValue, ((InternalMin) parent.getAggregations().get("in_parent")).getValue(), Double.MIN_VALUE);
            assertTrue(JoinAggregationInspectionHelper.hasValue(parent));
        });

        // verify for each children
        for (String parent : expectedParentChildRelations.keySet()) {
            testCase(new TermInSetQuery(IdFieldMapper.NAME, Uid.encodeId("child0_" + parent)),
                indexSearcher, aggregation -> {
                assertEquals("Expected one result for min-aggregation for parent: " + parent +
                        ", but had aggregation-results: " + aggregation,
                    1, aggregation.getDocCount());
                assertEquals(expectedParentChildRelations.get(parent).v2(),
                        ((InternalMin) aggregation.getAggregations().get("in_parent")).getValue(), Double.MIN_VALUE);
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
            entries.put(value.v2(), l+1);
        }
        List<Map.Entry<Integer, Long>> sortedValues = new ArrayList<>(entries.entrySet());
        sortedValues.sort((o1, o2) -> {
            // sort larger values first
            int ret = o2.getValue().compareTo(o1.getValue());
            if(ret != 0) {
                return ret;
            }

            // on equal value, sort by key
            return o1.getKey().compareTo(o2.getKey());
        });

        IndexReader indexReader = ElasticsearchDirectoryReader.wrap(DirectoryReader.open(directory),
            new ShardId(new Index("foo", "_na_"), 1));
        // TODO set "maybeWrap" to true for IndexSearcher once #23338 is resolved
        IndexSearcher indexSearcher = newSearcher(indexReader, false, true);

        // verify a terms-aggregation inside the parent-aggregation
        testCaseTerms(new MatchAllDocsQuery(), indexSearcher, parent -> {
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
                assertEquals(entry.getValue(), (Long)bucket.getDocCount());

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
            sortedValues.put(value.v2(), l+1);
        }

        IndexReader indexReader = ElasticsearchDirectoryReader.wrap(DirectoryReader.open(directory),
            new ShardId(new Index("foo", "_na_"), 1));
        // TODO set "maybeWrap" to true for IndexSearcher once #23338 is resolved
        IndexSearcher indexSearcher = newSearcher(indexReader, false, true);

        // verify a terms-aggregation inside the parent-aggregation which itself is inside a
        // terms-aggregation on the child-documents
        testCaseTermsParentTerms(new MatchAllDocsQuery(), indexSearcher, longTerms -> {
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

    @Override
    protected MapperService mapperServiceMock() {
        ParentJoinFieldMapper joinFieldMapper = createJoinFieldMapper();
        MapperService mapperService = mock(MapperService.class);
        MetaJoinFieldMapper.MetaJoinFieldType metaJoinFieldType = mock(MetaJoinFieldMapper.MetaJoinFieldType.class);
        when(metaJoinFieldType.getJoinField()).thenReturn("join_field");
        when(mapperService.fieldType("_parent_join")).thenReturn(metaJoinFieldType);
        MappingLookup fieldMappers = new MappingLookup(Collections.singleton(joinFieldMapper),
            Collections.emptyList(), Collections.emptyList(), 0, null);
        DocumentMapper mockMapper = mock(DocumentMapper.class);
        when(mockMapper.mappers()).thenReturn(fieldMappers);
        when(mapperService.documentMapper()).thenReturn(mockMapper);
        return mapperService;
    }

    private static ParentJoinFieldMapper createJoinFieldMapper() {
        Settings settings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT).build();
        return new ParentJoinFieldMapper.Builder("join_field")
                .addParent(PARENT_TYPE, Collections.singleton(CHILD_TYPE))
                .build(new Mapper.BuilderContext(settings, new ContentPath(0)));
    }

    private void testCase(Query query, IndexSearcher indexSearcher, Consumer<InternalParent> verify)
            throws IOException {

        ParentAggregationBuilder aggregationBuilder = new ParentAggregationBuilder("_name", CHILD_TYPE);
        aggregationBuilder.subAggregation(new MinAggregationBuilder("in_parent").field("number"));

        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("number", NumberFieldMapper.NumberType.LONG);
        InternalParent result = searchAndReduce(indexSearcher, query, aggregationBuilder, fieldType);
        verify.accept(result);
    }

    private void testCaseTerms(Query query, IndexSearcher indexSearcher, Consumer<InternalParent> verify)
            throws IOException {

        ParentAggregationBuilder aggregationBuilder = new ParentAggregationBuilder("_name", CHILD_TYPE);
        aggregationBuilder.subAggregation(new TermsAggregationBuilder("value_terms").field("number"));

        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("number", NumberFieldMapper.NumberType.LONG);
        InternalParent result = searchAndReduce(indexSearcher, query, aggregationBuilder, fieldType);
        verify.accept(result);
    }

    // run a terms aggregation on the number in child-documents, then a parent aggregation and then terms on the parent-number
    private void testCaseTermsParentTerms(Query query, IndexSearcher indexSearcher, Consumer<LongTerms> verify)
            throws IOException {
        AggregationBuilder aggregationBuilder =
            new TermsAggregationBuilder("subvalue_terms").field("subNumber").
                subAggregation(new ParentAggregationBuilder("to_parent", CHILD_TYPE).
                    subAggregation(new TermsAggregationBuilder("value_terms").field("number")));

        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("number", NumberFieldMapper.NumberType.LONG);
        MappedFieldType subFieldType = new NumberFieldMapper.NumberFieldType("subNumber", NumberFieldMapper.NumberType.LONG);
        LongTerms result = searchAndReduce(indexSearcher, query, aggregationBuilder, fieldType, subFieldType);
        verify.accept(result);
    }

    @Override
    protected List<SearchPlugin> getSearchPlugins() {
        return Collections.singletonList(new ParentJoinPlugin());
    }
}
