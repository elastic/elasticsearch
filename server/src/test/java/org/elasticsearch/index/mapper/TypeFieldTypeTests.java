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
package org.elasticsearch.index.mapper;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.test.VersionUtils;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;

import static org.hamcrest.Matchers.instanceOf;

public class TypeFieldTypeTests extends FieldTypeTestCase {
    @Override
    protected MappedFieldType createDefaultFieldType() {
        return new TypeFieldMapper.TypeFieldType();
    }

    private QueryShardContext createMockContext(Version versionFrom, Version versionTo) {
        QueryShardContext context = Mockito.mock(QueryShardContext.class);
        Version indexVersionCreated = VersionUtils.randomVersionBetween(random(), versionFrom, versionTo);
        Settings indexSettings = Settings.builder()
                .put(IndexMetaData.SETTING_VERSION_CREATED, indexVersionCreated)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_INDEX_UUID, UUIDs.randomBase64UUID()).build();
        IndexMetaData indexMetaData = IndexMetaData.builder(IndexMetaData.INDEX_UUID_NA_VALUE).settings(indexSettings).build();
        IndexSettings mockSettings = new IndexSettings(indexMetaData, Settings.EMPTY);
        Mockito.when(context.getIndexSettings()).thenReturn(mockSettings);
        Mockito.when(context.indexVersionCreated()).thenReturn(indexVersionCreated);

        return context;
    }

    public void testTermsQueryWhenTypesAreDisabled() throws Exception {
        QueryShardContext context = createMockContext(Version.V_6_0_0, Version.CURRENT);

        MapperService mapperService = Mockito.mock(MapperService.class);
        Set<String> types = Collections.emptySet();
        Mockito.when(mapperService.types()).thenReturn(types);
        Mockito.when(context.getMapperService()).thenReturn(mapperService);

        TypeFieldMapper.TypeFieldType ft = new TypeFieldMapper.TypeFieldType();
        ft.setName(TypeFieldMapper.NAME);
        Query query = ft.termQuery("my_type", context);
        assertEquals(new MatchNoDocsQuery(), query);

        types = Collections.singleton("my_type");
        Mockito.when(mapperService.types()).thenReturn(types);
        query = ft.termQuery("my_type", context);
        assertEquals(new MatchAllDocsQuery(), query);

        Mockito.when(mapperService.hasNested()).thenReturn(true);
        query = ft.termQuery("my_type", context);
        assertEquals(Queries.newNonNestedFilter(context.indexVersionCreated()), query);

        types = Collections.singleton("other_type");
        Mockito.when(mapperService.types()).thenReturn(types);
        query = ft.termQuery("my_type", context);
        assertEquals(new MatchNoDocsQuery(), query);
    }

    public void testTermsQueryWhenTypesAreEnabled() throws Exception {
        Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
        IndexReader reader = openReaderWithNewType("my_type", w);

        QueryShardContext context = createMockContext(Version.V_5_6_0, Version.V_5_6_0); // to allow for multiple types

        TypeFieldMapper.TypeFieldType ft = new TypeFieldMapper.TypeFieldType();
        ft.setName(TypeFieldMapper.NAME);
        Query query = ft.termQuery("my_type", context);
        assertEquals(new MatchAllDocsQuery(), query.rewrite(reader));

        // Make sure that Lucene actually simplifies the query when there is a single type
        Query userQuery = new PhraseQuery("body", "quick", "fox");
        Query filteredQuery = new BooleanQuery.Builder().add(userQuery, Occur.MUST).add(query, Occur.FILTER).build();
        Query rewritten = new IndexSearcher(reader).rewrite(filteredQuery);
        assertEquals(userQuery, rewritten);

        // ... and does not rewrite it if there is more than one type
        reader.close();
        reader = openReaderWithNewType("my_type2", w);
        Query expected = new ConstantScoreQuery(
            new BooleanQuery.Builder()
                .add(new TermQuery(new Term(TypeFieldMapper.NAME, "my_type")), Occur.SHOULD)
            .build()
        );
        assertEquals(expected, query.rewrite(reader));

        BytesRef[] types =
            new BytesRef[] {new BytesRef("my_type"), new BytesRef("my_type2"), new BytesRef("my_type3")};
        // the query should match all documents
        query = new TypeFieldMapper.TypesQuery(types);
        assertEquals(new MatchAllDocsQuery(), query.rewrite(reader));

        reader.close();
        reader = openReaderWithNewType("unknown_type", w);
        // the query cannot rewrite to a match all docs sinc unknown_type is not queried.
        query = new TypeFieldMapper.TypesQuery(types);
        expected =
            new ConstantScoreQuery(
                new BooleanQuery.Builder()
                    .add(new TermQuery(new Term(TypeFieldMapper.CONTENT_TYPE, types[0])), Occur.SHOULD)
                    .add(new TermQuery(new Term(TypeFieldMapper.CONTENT_TYPE, types[1])), Occur.SHOULD)
                .build()
            );
        rewritten = query.rewrite(reader);
        assertEquals(expected, rewritten);

        // make sure that redundant types does not rewrite to MatchAllDocsQuery
        query = new TypeFieldMapper.TypesQuery(new BytesRef("my_type"), new BytesRef("my_type"), new BytesRef("my_type"));
        expected =
            new ConstantScoreQuery(
                new BooleanQuery.Builder()
                    .add(new TermQuery(new Term(TypeFieldMapper.CONTENT_TYPE, "my_type")), Occur.SHOULD)
                    .build()
            );
        rewritten = query.rewrite(reader);
        assertEquals(expected, rewritten);

        IOUtils.close(reader, w, dir);
    }

    public void testRangeWhenTypesAreDisabled() throws Exception {
        QueryShardContext context = createMockContext(Version.V_6_0_0, Version.CURRENT);

        MapperService mapperService = Mockito.mock(MapperService.class);
        Set<String> types = Collections.emptySet();
        Mockito.when(mapperService.types()).thenReturn(types);
        Mockito.when(context.getMapperService()).thenReturn(mapperService);

        TypeFieldMapper.TypeFieldType ft = new TypeFieldMapper.TypeFieldType();
        ft.setName(TypeFieldMapper.NAME);
        Query query = ft.rangeQuery("a_type", "z_type", randomBoolean(), randomBoolean(), context);
        assertEquals(new MatchNoDocsQuery(), query);

        types = Collections.singleton("my_type");
        Mockito.when(mapperService.types()).thenReturn(types);
        query = ft.rangeQuery("a_type", "z_type", randomBoolean(), randomBoolean(), context);
        assertEquals(new MatchAllDocsQuery(), query);

        query = ft.rangeQuery("n_type", "z_type", randomBoolean(), randomBoolean(), context);
        assertEquals(new MatchNoDocsQuery(), query);

        query = ft.rangeQuery("a_type", "l_type", randomBoolean(), randomBoolean(), context);
        assertEquals(new MatchNoDocsQuery(), query);
        assertWarnings("Running [range] query on [_type] field for an index with a single type. As types are deprecated, this "
                + "functionality will be removed in future releases.");
    }

    public void testRangeWhenTypesEnabled() throws Exception {
        TypeFieldMapper.TypeFieldType ft = new TypeFieldMapper.TypeFieldType();
        ft.setName(TypeFieldMapper.NAME);
        String lowerTerm = randomBoolean() ? "a_type" : null;
        String upperTerm = randomBoolean() ? "z_type" : null;
        QueryShardContext context = createMockContext(Version.V_5_6_0, Version.V_5_6_0); // to allow for multiple types
        Query query = ft.rangeQuery(lowerTerm, upperTerm, randomBoolean(), randomBoolean(), context);
        assertThat(query, instanceOf(TermRangeQuery.class));
    }

    static DirectoryReader openReaderWithNewType(String type, IndexWriter writer) throws IOException {
        Document doc = new Document();
        StringField typeField = new StringField(TypeFieldMapper.NAME, type, Store.NO);
        doc.add(typeField);
        writer.addDocument(doc);
        return DirectoryReader.open(writer);
    }
}
