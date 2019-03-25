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
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.ContentPath;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.join.mapper.MetaJoinFieldMapper;
import org.elasticsearch.join.mapper.ParentJoinFieldMapper;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.metrics.InternalMin;
import org.elasticsearch.search.aggregations.metrics.MinAggregationBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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

    private static Map<String, Tuple<Integer, Integer>> setupIndex(RandomIndexWriter iw) throws IOException {
        Map<String, Tuple<Integer, Integer>> expectedValues = new HashMap<>();
        int numParents = randomIntBetween(1, 10);
        for (int i = 0; i < numParents; i++) {
            String parent = "parent" + i;
            iw.addDocument(createParentDocument(parent));
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

    private static List<Field> createParentDocument(String id) {
        return Arrays.asList(
                new StringField(IdFieldMapper.NAME, Uid.encodeId(id), Field.Store.NO),
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

    @Override
    protected MapperService mapperServiceMock() {
        ParentJoinFieldMapper joinFieldMapper = createJoinFieldMapper();
        MapperService mapperService = mock(MapperService.class);
        MetaJoinFieldMapper.MetaJoinFieldType metaJoinFieldType = mock(MetaJoinFieldMapper.MetaJoinFieldType.class);
        when(metaJoinFieldType.getMapper()).thenReturn(joinFieldMapper);
        when(mapperService.fullName("_parent_join")).thenReturn(metaJoinFieldType);
        return mapperService;
    }

    private static ParentJoinFieldMapper createJoinFieldMapper() {
        Settings settings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT).build();
        return new ParentJoinFieldMapper.Builder("join_field")
                .addParent(PARENT_TYPE, Collections.singleton(CHILD_TYPE))
                .build(new Mapper.BuilderContext(settings, new ContentPath(0)));
    }

    private void testCase(Query query, IndexSearcher indexSearcher, Consumer<InternalChildren> verify)
            throws IOException {

        ChildrenAggregationBuilder aggregationBuilder = new ChildrenAggregationBuilder("_name", CHILD_TYPE);
        aggregationBuilder.subAggregation(new MinAggregationBuilder("in_child").field("number"));

        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType(NumberFieldMapper.NumberType.LONG);
        fieldType.setName("number");
        InternalChildren result = search(indexSearcher, query, aggregationBuilder, fieldType);
        verify.accept(result);
    }
}
