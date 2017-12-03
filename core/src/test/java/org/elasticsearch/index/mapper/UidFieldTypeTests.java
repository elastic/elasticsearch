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

import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.UidFieldMapper;
import org.elasticsearch.index.query.QueryShardContext;
import org.mockito.Mockito;

import java.util.Collection;
import java.util.Collections;

public class UidFieldTypeTests extends FieldTypeTestCase {
    @Override
    protected MappedFieldType createDefaultFieldType() {
        return new UidFieldMapper.UidFieldType();
    }

    public void testRangeQuery() {
        MappedFieldType ft = createDefaultFieldType();
        ft.setName("_uid");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> ft.rangeQuery(null, null, randomBoolean(), randomBoolean(), null, null, null, null));
        assertEquals("Field [_uid] of type [_uid] does not support range queries", e.getMessage());
    }

    public void testTermsQueryWhenTypesAreEnabled() throws Exception {
        QueryShardContext context = Mockito.mock(QueryShardContext.class);
        Settings indexSettings = Settings.builder()
                .put(IndexMetaData.SETTING_VERSION_CREATED, Version.V_5_6_0) // to allow for multipel types
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_INDEX_UUID, UUIDs.randomBase64UUID())
                .build();
        IndexMetaData indexMetaData = IndexMetaData.builder(IndexMetaData.INDEX_UUID_NA_VALUE).settings(indexSettings).build();
        IndexSettings mockSettings = new IndexSettings(indexMetaData, Settings.EMPTY);
        Mockito.when(context.getIndexSettings()).thenReturn(mockSettings);

        MapperService mapperService = Mockito.mock(MapperService.class);
        Collection<String> types = Collections.emptySet();
        Mockito.when(context.queryTypes()).thenReturn(types);
        Mockito.when(context.getMapperService()).thenReturn(mapperService);

        MappedFieldType ft = UidFieldMapper.defaultFieldType(mockSettings);
        ft.setName(UidFieldMapper.NAME);
        Query query = ft.termQuery("type#id", context);
        assertEquals(new TermInSetQuery("_uid", new BytesRef("type#id")), query);
    }

    public void testTermsQueryWhenTypesAreDisabled() throws Exception {
        QueryShardContext context = Mockito.mock(QueryShardContext.class);
        Settings indexSettings = Settings.builder()
                .put(IndexSettings.INDEX_MAPPING_SINGLE_TYPE_SETTING_KEY, true)
                .put(IndexMetaData.SETTING_VERSION_CREATED, Version.V_5_6_0)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_INDEX_UUID, UUIDs.randomBase64UUID()).build();
        IndexMetaData indexMetaData = IndexMetaData.builder(IndexMetaData.INDEX_UUID_NA_VALUE).settings(indexSettings).build();
        IndexSettings mockSettings = new IndexSettings(indexMetaData, Settings.EMPTY);
        Mockito.when(context.getIndexSettings()).thenReturn(mockSettings);

        MapperService mapperService = Mockito.mock(MapperService.class);
        Collection<String> types = Collections.emptySet();
        Mockito.when(mapperService.types()).thenReturn(types);
        Mockito.when(context.getMapperService()).thenReturn(mapperService);
        Mockito.when(context.indexVersionCreated()).thenReturn(indexSettings.getAsVersion(IndexMetaData.SETTING_VERSION_CREATED, null));

        MappedFieldType ft = UidFieldMapper.defaultFieldType(mockSettings);
        ft.setName(UidFieldMapper.NAME);
        Query query = ft.termQuery("type#id", context);
        assertEquals(new MatchNoDocsQuery(), query);

        types = Collections.singleton("type");
        Mockito.when(mapperService.types()).thenReturn(types);
        query = ft.termQuery("type#id", context);
        assertEquals(new TermInSetQuery("_id", new BytesRef("id")), query);
        query = ft.termQuery("type2#id", context);
        assertEquals(new TermInSetQuery("_id"), query);
    }

    public void testTermsQuery() throws Exception {
        QueryShardContext context = Mockito.mock(QueryShardContext.class);
        Settings indexSettings = Settings.builder()
                .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_INDEX_UUID, UUIDs.randomBase64UUID()).build();
        IndexMetaData indexMetaData = IndexMetaData.builder(IndexMetaData.INDEX_UUID_NA_VALUE).settings(indexSettings).build();
        IndexSettings mockSettings = new IndexSettings(indexMetaData, Settings.EMPTY);
        Mockito.when(context.getIndexSettings()).thenReturn(mockSettings);

        MapperService mapperService = Mockito.mock(MapperService.class);
        Collection<String> types = Collections.emptySet();
        Mockito.when(mapperService.types()).thenReturn(types);
        Mockito.when(context.getMapperService()).thenReturn(mapperService);
        Mockito.when(context.indexVersionCreated()).thenReturn(indexSettings.getAsVersion(IndexMetaData.SETTING_VERSION_CREATED, null));

        MappedFieldType ft = UidFieldMapper.defaultFieldType(mockSettings);
        ft.setName(UidFieldMapper.NAME);
        Query query = ft.termQuery("type#id", context);
        assertEquals(new MatchNoDocsQuery(), query);

        types = Collections.singleton("type");
        Mockito.when(mapperService.types()).thenReturn(types);
        query = ft.termQuery("type#id", context);
        assertEquals(new TermInSetQuery("_id", Uid.encodeId("id")), query);
        query = ft.termQuery("type2#id", context);
        assertEquals(new TermInSetQuery("_id"), query);
    }
}
