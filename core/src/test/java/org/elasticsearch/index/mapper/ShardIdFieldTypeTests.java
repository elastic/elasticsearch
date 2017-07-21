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

import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.query.QueryShardContext;
import org.mockito.Mockito;

public class ShardIdFieldTypeTests extends FieldTypeTestCase {

    @Override
    protected MappedFieldType createDefaultFieldType() {
        return new ShardIdFieldMapper.ShardIdFieldType();
    }

    public void testTermQuery() {
        QueryShardContext context = Mockito.mock(QueryShardContext.class);
        Settings indexSettings = Settings.builder()
                .put(IndexSettings.INDEX_MAPPING_SINGLE_TYPE_SETTING_KEY, true)
                .put(IndexMetaData.SETTING_VERSION_CREATED, Version.V_6_0_0_beta1)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_INDEX_UUID, UUIDs.randomBase64UUID()).build();
        IndexMetaData indexMetaData = IndexMetaData.builder(IndexMetaData.INDEX_UUID_NA_VALUE).settings(indexSettings).build();
        IndexSettings mockSettings = new IndexSettings(indexMetaData, Settings.EMPTY);
        Mockito.when(context.getIndexSettings()).thenReturn(mockSettings);
        Mockito.when(context.indexVersionCreated()).thenReturn(indexSettings.getAsVersion(IndexMetaData.SETTING_VERSION_CREATED, null));
        Mockito.when(context.getShardId()).thenReturn(3);
        MappedFieldType ft = createDefaultFieldType();
        Query query = ft.termQuery(1, context);
        assertEquals(SortedDocValuesField.newExactQuery("_shard", new BytesRef("1")), query);
    }

    public void testTermQueryBwCompat() {
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
        Mockito.when(context.indexVersionCreated()).thenReturn(indexSettings.getAsVersion(IndexMetaData.SETTING_VERSION_CREATED, null));
        Mockito.when(context.getShardId()).thenReturn(3);
        MappedFieldType ft = createDefaultFieldType();
        Query query = ft.termQuery(1, context);
        assertEquals(new MatchNoDocsQuery(), query);
        query = ft.termQuery(3, context);
        assertEquals(new MatchAllDocsQuery(), query);
    }
}
