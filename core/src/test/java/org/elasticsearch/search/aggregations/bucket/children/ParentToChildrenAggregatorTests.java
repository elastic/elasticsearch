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

package org.elasticsearch.search.aggregations.bucket.children;

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
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.ContentPath;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.mapper.ParentFieldMapper;
import org.elasticsearch.index.mapper.TypeFieldMapper;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.UidFieldMapper;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.metrics.min.InternalMin;
import org.elasticsearch.search.aggregations.metrics.min.MinAggregationBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ParentToChildrenAggregatorTests extends AggregatorTestCase {

    private static final String CHILD_TYPE = "child_type";
    private static final String PARENT_TYPE = "parent_type";

    public void testNoDocs() throws IOException {
        testCase(new MatchAllDocsQuery(), iw -> {
            // Intentionally not writing any docs
        }, parentToChild -> {
            assertEquals(0, parentToChild.getDocCount());
            assertEquals(Double.POSITIVE_INFINITY, ((InternalMin) parentToChild.getAggregations().get("in_child")).getValue(),
                    Double.MIN_VALUE);
        });
    }

    public void testParentChild() throws IOException {
        testCase(new MatchAllDocsQuery(), ParentToChildrenAggregatorTests::setupIndex, child -> {
            assertEquals(4, child.getDocCount());
            assertEquals(2, ((InternalMin) child.getAggregations().get("in_child")).getValue(), Double.MIN_VALUE);
        });

        testCase(new TermInSetQuery(UidFieldMapper.NAME, new BytesRef(Uid.createUid(PARENT_TYPE, "parent1"))),
                ParentToChildrenAggregatorTests::setupIndex, child -> {
                    assertEquals(1, child.getDocCount());
                    assertEquals(10, ((InternalMin) child.getAggregations().get("in_child")).getValue(), Double.MIN_VALUE);
                });

        testCase(new TermInSetQuery(UidFieldMapper.NAME, new BytesRef(Uid.createUid(PARENT_TYPE, "parent2"))),
                ParentToChildrenAggregatorTests::setupIndex, child -> {
                    assertEquals(3, child.getDocCount());
                    assertEquals(2, ((InternalMin) child.getAggregations().get("in_child")).getValue(), Double.MIN_VALUE);
                });
    }

    private static void setupIndex(RandomIndexWriter iw) throws IOException {
        iw.addDocument(createParentDocument("parent1"));
        iw.addDocument(createChildDocument("child1", "parent1", 10));

        iw.addDocument(createParentDocument("parent2"));
        iw.addDocument(createChildDocument("child2", "parent2", 5));
        iw.addDocument(createChildDocument("child3", "parent2", 2));
        iw.addDocument(createChildDocument("child4", "parent2", 7));
    }

    private static List<Field> createParentDocument(String id) {
        return Arrays.asList(new StringField(TypeFieldMapper.NAME, PARENT_TYPE, Field.Store.NO),
                new StringField(UidFieldMapper.NAME, Uid.createUid(PARENT_TYPE, id), Field.Store.NO),
                createJoinField(PARENT_TYPE, id));
    }

    private static List<Field> createChildDocument(String childId, String parentId, int value) {
        return Arrays.asList(new StringField(TypeFieldMapper.NAME, CHILD_TYPE, Field.Store.NO),
                new StringField(UidFieldMapper.NAME, Uid.createUid(CHILD_TYPE, childId), Field.Store.NO),
                new SortedNumericDocValuesField("number", value),
                createJoinField(PARENT_TYPE, parentId));
    }

    private static SortedDocValuesField createJoinField(String parentType, String id) {
        return new SortedDocValuesField(ParentFieldMapper.joinField(parentType), new BytesRef(id));
    }

    @Override
    protected MapperService mapperServiceMock() {
        MapperService mapperService = mock(MapperService.class);
        DocumentMapper childDocMapper = mock(DocumentMapper.class);
        DocumentMapper parentDocMapper = mock(DocumentMapper.class);
        ParentFieldMapper parentFieldMapper = createParentFieldMapper();
        when(childDocMapper.parentFieldMapper()).thenReturn(parentFieldMapper);
        when(parentDocMapper.parentFieldMapper()).thenReturn(parentFieldMapper);
        when(mapperService.documentMapper(CHILD_TYPE)).thenReturn(childDocMapper);
        when(mapperService.documentMapper(PARENT_TYPE)).thenReturn(parentDocMapper);
        when(mapperService.docMappers(false)).thenReturn(Arrays.asList(new DocumentMapper[] { childDocMapper, parentDocMapper }));
        when(parentDocMapper.typeFilter()).thenReturn(new TypeFieldMapper.TypesQuery(new BytesRef(PARENT_TYPE)));
        when(childDocMapper.typeFilter()).thenReturn(new TypeFieldMapper.TypesQuery(new BytesRef(CHILD_TYPE)));
        return mapperService;
    }

    private static ParentFieldMapper createParentFieldMapper() {
        Settings settings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT).build();
        return new ParentFieldMapper.Builder("parent").type(PARENT_TYPE).build(new Mapper.BuilderContext(settings, new ContentPath(0)));
    }

    private void testCase(Query query, CheckedConsumer<RandomIndexWriter, IOException> buildIndex, Consumer<InternalChildren> verify)
            throws IOException {
        Directory directory = newDirectory();
        RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);
        buildIndex.accept(indexWriter);
        indexWriter.close();

        IndexReader indexReader = DirectoryReader.open(directory);
        // TODO no "maybeWrap" for the searcher because this randomly led to java.lang.ClassCastException:
        // org.apache.lucene.search.QueryUtils$FCInvisibleMultiReader cannot be
        // cast to org.apache.lucene.index.DirectoryReader
        // according to @mvg this can be fixed later but requires bigger changes
        IndexSearcher indexSearcher = newSearcher(indexReader, false, true);

        ChildrenAggregationBuilder aggregationBuilder = new ChildrenAggregationBuilder("_name", CHILD_TYPE);
        aggregationBuilder.subAggregation(new MinAggregationBuilder("in_child").field("number"));

        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType(NumberFieldMapper.NumberType.LONG);
        fieldType.setName("number");
        InternalChildren result = search(indexSearcher, query, aggregationBuilder, fieldType);
        verify.accept(result);
        indexReader.close();
        directory.close();
    }
}
