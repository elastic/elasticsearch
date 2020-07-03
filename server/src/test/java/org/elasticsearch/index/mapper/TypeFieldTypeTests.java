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

import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.test.ESTestCase;
import org.mockito.Mockito;

public class TypeFieldTypeTests extends ESTestCase {

    public void testTermsQuery() {
        QueryShardContext context = Mockito.mock(QueryShardContext.class);
        Version indexVersionCreated = VersionUtils.randomVersionBetween(random(), Version.V_6_0_0, Version.CURRENT);
        Settings indexSettings = Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, indexVersionCreated)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_INDEX_UUID, UUIDs.randomBase64UUID()).build();
        IndexMetadata indexMetadata = IndexMetadata.builder(IndexMetadata.INDEX_UUID_NA_VALUE).settings(indexSettings).build();
        IndexSettings mockSettings = new IndexSettings(indexMetadata, Settings.EMPTY);
        Mockito.when(context.getIndexSettings()).thenReturn(mockSettings);
        Mockito.when(context.indexVersionCreated()).thenReturn(indexVersionCreated);

        MapperService mapperService = Mockito.mock(MapperService.class);
        Mockito.when(mapperService.documentMapper()).thenReturn(null);
        Mockito.when(context.getMapperService()).thenReturn(mapperService);

        TypeFieldMapper.TypeFieldType ft = TypeFieldMapper.TypeFieldType.INSTANCE;
        Query query = ft.termQuery("my_type", context);
        assertEquals(new MatchNoDocsQuery(), query);

        DocumentMapper mapper = Mockito.mock(DocumentMapper.class);
        Mockito.when(mapper.type()).thenReturn("my_type");
        Mockito.when(mapperService.documentMapper()).thenReturn(mapper);
        query = ft.termQuery("my_type", context);
        assertEquals(new MatchAllDocsQuery(), query);

        Mockito.when(mapperService.hasNested()).thenReturn(true);
        query = ft.termQuery("my_type", context);
        assertEquals(Queries.newNonNestedFilter(context.indexVersionCreated()), query);

        mapper = Mockito.mock(DocumentMapper.class);
        Mockito.when(mapper.type()).thenReturn("other_type");
        Mockito.when(mapperService.documentMapper()).thenReturn(mapper);
        query = ft.termQuery("my_type", context);
        assertEquals(new MatchNoDocsQuery(), query);
    }
}
