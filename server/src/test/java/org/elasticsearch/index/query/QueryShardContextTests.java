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
package org.elasticsearch.index.query;

import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.plain.AbstractAtomicOrdinalsFieldData;
import org.elasticsearch.index.mapper.ContentPath;
import org.elasticsearch.index.mapper.IndexFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.TextFieldMapper;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class QueryShardContextTests extends ESTestCase {

    public void testFailIfFieldMappingNotFound() {
        IndexMetaData.Builder indexMetadataBuilder = new IndexMetaData.Builder("index");
        indexMetadataBuilder.settings(Settings.builder().put("index.version.created", Version.CURRENT)
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 1)
        );
        IndexMetaData indexMetaData = indexMetadataBuilder.build();
        IndexSettings indexSettings = new IndexSettings(indexMetaData, Settings.EMPTY);
        MapperService mapperService = mock(MapperService.class);
        when(mapperService.getIndexSettings()).thenReturn(indexSettings);
        when(mapperService.index()).thenReturn(indexMetaData.getIndex());
        final long nowInMillis = randomNonNegativeLong();

        QueryShardContext context = new QueryShardContext(
            0, indexSettings, null, (mappedFieldType, idxName) ->
                mappedFieldType.fielddataBuilder(idxName).build(indexSettings, mappedFieldType, null, null, null)
                , mapperService, null, null, xContentRegistry(), writableRegistry(), null, null,
            () -> nowInMillis, null);

        context.setAllowUnmappedFields(false);
        MappedFieldType fieldType = new TextFieldMapper.TextFieldType();
        MappedFieldType result = context.failIfFieldMappingNotFound("name", fieldType);
        assertThat(result, sameInstance(fieldType));
        QueryShardException e = expectThrows(QueryShardException.class, () -> context.failIfFieldMappingNotFound("name", null));
        assertEquals("No field mapping can be found for the field with name [name]", e.getMessage());

        context.setAllowUnmappedFields(true);
        result = context.failIfFieldMappingNotFound("name", fieldType);
        assertThat(result, sameInstance(fieldType));
        result = context.failIfFieldMappingNotFound("name", null);
        assertThat(result, nullValue());

        context.setAllowUnmappedFields(false);
        context.setMapUnmappedFieldAsString(true);
        result = context.failIfFieldMappingNotFound("name", fieldType);
        assertThat(result, sameInstance(fieldType));
        result = context.failIfFieldMappingNotFound("name", null);
        assertThat(result, notNullValue());
        assertThat(result, instanceOf(TextFieldMapper.TextFieldType.class));
        assertThat(result.name(), equalTo("name"));
    }

    public void testClusterAlias() throws IOException {
        IndexMetaData.Builder indexMetadataBuilder = new IndexMetaData.Builder("index");
        indexMetadataBuilder.settings(Settings.builder().put("index.version.created", Version.CURRENT)
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 1)
        );
        IndexMetaData indexMetaData = indexMetadataBuilder.build();
        IndexSettings indexSettings = new IndexSettings(indexMetaData, Settings.EMPTY);
        MapperService mapperService = mock(MapperService.class);
        when(mapperService.getIndexSettings()).thenReturn(indexSettings);
        when(mapperService.index()).thenReturn(indexMetaData.getIndex());
        final long nowInMillis = randomNonNegativeLong();

        Mapper.BuilderContext ctx = new Mapper.BuilderContext(indexSettings.getSettings(), new ContentPath());
        IndexFieldMapper mapper = new IndexFieldMapper.Builder(null).build(ctx);
        final String clusterAlias = randomBoolean() ? null : "remote_cluster";
        QueryShardContext context = new QueryShardContext(
            0, indexSettings, null, (mappedFieldType, indexname) ->
            mappedFieldType.fielddataBuilder(indexname).build(indexSettings, mappedFieldType, null, null, mapperService)
            , mapperService, null, null, xContentRegistry(), writableRegistry(), null, null,
            () -> nowInMillis, clusterAlias);

        IndexFieldData<?> forField = context.getForField(mapper.fieldType());
        String expected = clusterAlias == null ? indexMetaData.getIndex().getName()
            : clusterAlias + ":" + indexMetaData.getIndex().getName();
        assertEquals(expected, ((AbstractAtomicOrdinalsFieldData)forField.load(null)).getOrdinalsValues().lookupOrd(0).utf8ToString());
        Query query = mapper.fieldType().termQuery("index", context);
        if (clusterAlias == null) {
            assertEquals(Queries.newMatchAllQuery(), query);
        } else {
            assertThat(query, Matchers.instanceOf(MatchNoDocsQuery.class));
        }
        query = mapper.fieldType().termQuery("remote_cluster:index", context);
        if (clusterAlias != null) {
            assertEquals(Queries.newMatchAllQuery(), query);
        } else {
            assertThat(query, Matchers.instanceOf(MatchNoDocsQuery.class));
        }

        query = mapper.fieldType().termQuery("something:else", context);
        assertThat(query, Matchers.instanceOf(MatchNoDocsQuery.class));
    }

}
