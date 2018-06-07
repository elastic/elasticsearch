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

package org.elasticsearch.client;

import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesResponse;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.cache.clear.ClearIndicesCacheRequest;
import org.elasticsearch.action.admin.indices.cache.clear.ClearIndicesCacheResponse;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.admin.indices.close.CloseIndexResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.admin.indices.flush.FlushResponse;
import org.elasticsearch.action.admin.indices.flush.SyncedFlushRequest;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.action.admin.indices.rollover.RolloverResponse;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsResponse;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.admin.indices.shrink.ResizeRequest;
import org.elasticsearch.action.admin.indices.shrink.ResizeResponse;
import org.elasticsearch.action.admin.indices.shrink.ResizeType;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.extractRawValues;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.extractValue;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;

public class IndicesClientIT extends ESRestHighLevelClientTestCase {

    public void testIndicesExists() throws IOException {
        // Index present
        {
            String indexName = "test_index_exists_index_present";
            createIndex(indexName, Settings.EMPTY);

            GetIndexRequest request = new GetIndexRequest();
            request.indices(indexName);

            boolean response = execute(
                request,
                highLevelClient().indices()::exists,
                highLevelClient().indices()::existsAsync,
                highLevelClient().indices()::exists,
                highLevelClient().indices()::existsAsync
            );
            assertTrue(response);
        }

        // Index doesn't exist
        {
            String indexName = "non_existent_index";

            GetIndexRequest request = new GetIndexRequest();
            request.indices(indexName);

            boolean response = execute(
                request,
                highLevelClient().indices()::exists,
                highLevelClient().indices()::existsAsync,
                highLevelClient().indices()::exists,
                highLevelClient().indices()::existsAsync
            );
            assertFalse(response);
        }

        // One index exists, one doesn't
        {
            String existingIndex = "apples";
            createIndex(existingIndex, Settings.EMPTY);

            String nonExistentIndex = "oranges";

            GetIndexRequest request = new GetIndexRequest();
            request.indices(existingIndex, nonExistentIndex);

            boolean response = execute(
                request,
                highLevelClient().indices()::exists,
                highLevelClient().indices()::existsAsync,
                highLevelClient().indices()::exists,
                highLevelClient().indices()::existsAsync
            );
            assertFalse(response);
        }

    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public void testCreateIndex() throws IOException {
        {
            // Create index
            String indexName = "plain_index";
            assertFalse(indexExists(indexName));

            CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName);

            CreateIndexResponse createIndexResponse =
                    execute(createIndexRequest, highLevelClient().indices()::create, highLevelClient().indices()::createAsync,
                            highLevelClient().indices()::create, highLevelClient().indices()::createAsync);
            assertTrue(createIndexResponse.isAcknowledged());

            assertTrue(indexExists(indexName));
        }
        {
            // Create index with mappings, aliases and settings
            String indexName = "rich_index";
            assertFalse(indexExists(indexName));

            CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName);

            Alias alias = new Alias("alias_name");
            alias.filter("{\"term\":{\"year\":2016}}");
            alias.routing("1");
            createIndexRequest.alias(alias);

            Settings.Builder settings = Settings.builder();
            settings.put(SETTING_NUMBER_OF_REPLICAS, 2);
            createIndexRequest.settings(settings);

            XContentBuilder mappingBuilder = JsonXContent.contentBuilder();
            mappingBuilder.startObject().startObject("properties").startObject("field");
            mappingBuilder.field("type", "text");
            mappingBuilder.endObject().endObject().endObject();
            createIndexRequest.mapping("type_name", mappingBuilder);

            CreateIndexResponse createIndexResponse =
                    execute(createIndexRequest, highLevelClient().indices()::create, highLevelClient().indices()::createAsync,
                            highLevelClient().indices()::create, highLevelClient().indices()::createAsync);
            assertTrue(createIndexResponse.isAcknowledged());

            Map<String, Object> getIndexResponse = getAsMap(indexName);
            assertEquals("2", XContentMapValues.extractValue(indexName + ".settings.index.number_of_replicas", getIndexResponse));

            Map<String, Object> aliasData =
                    (Map<String, Object>)XContentMapValues.extractValue(indexName + ".aliases.alias_name", getIndexResponse);
            assertNotNull(aliasData);
            assertEquals("1", aliasData.get("index_routing"));
            Map<String, Object> filter = (Map) aliasData.get("filter");
            Map<String, Object> term = (Map) filter.get("term");
            assertEquals(2016, term.get("year"));

            assertEquals("text", XContentMapValues.extractValue(indexName + ".mappings.type_name.properties.field.type", getIndexResponse));
        }
    }

    public void testGetSettings() throws IOException {
        String indexName = "get_settings_index";
        Settings basicSettings = Settings.builder()
            .put("number_of_shards", 1)
            .put("number_of_replicas", 0)
            .build();
        createIndex(indexName, basicSettings);

        GetSettingsRequest getSettingsRequest = new GetSettingsRequest().indices(indexName);
        GetSettingsResponse getSettingsResponse = execute(getSettingsRequest, highLevelClient().indices()::getSettings,
            highLevelClient().indices()::getSettingsAsync);

        assertNull(getSettingsResponse.getSetting(indexName, "index.refresh_interval"));
        assertEquals("1", getSettingsResponse.getSetting(indexName, "index.number_of_shards"));

        updateIndexSettings(indexName, Settings.builder().put("refresh_interval", "30s"));

        GetSettingsResponse updatedResponse = execute(getSettingsRequest, highLevelClient().indices()::getSettings,
            highLevelClient().indices()::getSettingsAsync);
        assertEquals("30s", updatedResponse.getSetting(indexName, "index.refresh_interval"));
    }

    public void testGetSettingsNonExistentIndex() throws IOException {
        String nonExistentIndex = "index_that_doesnt_exist";
        assertFalse(indexExists(nonExistentIndex));

        GetSettingsRequest getSettingsRequest = new GetSettingsRequest().indices(nonExistentIndex);
        ElasticsearchException exception = expectThrows(ElasticsearchException.class,
            () -> execute(getSettingsRequest, highLevelClient().indices()::getSettings, highLevelClient().indices()::getSettingsAsync));
        assertEquals(RestStatus.NOT_FOUND, exception.status());
    }

    public void testGetSettingsFromMultipleIndices() throws IOException {
        String indexName1 = "get_multiple_settings_one";
        createIndex(indexName1, Settings.builder().put("number_of_shards", 2).build());

        String indexName2 = "get_multiple_settings_two";
        createIndex(indexName2, Settings.builder().put("number_of_shards", 3).build());

        GetSettingsRequest getSettingsRequest = new GetSettingsRequest().indices("get_multiple_settings*");
        GetSettingsResponse getSettingsResponse = execute(getSettingsRequest, highLevelClient().indices()::getSettings,
            highLevelClient().indices()::getSettingsAsync);

        assertEquals("2", getSettingsResponse.getSetting(indexName1, "index.number_of_shards"));
        assertEquals("3", getSettingsResponse.getSetting(indexName2, "index.number_of_shards"));
    }

    public void testGetSettingsFiltered() throws IOException {
        String indexName = "get_settings_index";
        Settings basicSettings = Settings.builder()
            .put("number_of_shards", 1)
            .put("number_of_replicas", 0)
            .build();
        createIndex(indexName, basicSettings);

        GetSettingsRequest getSettingsRequest = new GetSettingsRequest().indices(indexName).names("index.number_of_shards");
        GetSettingsResponse getSettingsResponse = execute(getSettingsRequest, highLevelClient().indices()::getSettings,
            highLevelClient().indices()::getSettingsAsync);

        assertNull(getSettingsResponse.getSetting(indexName, "index.number_of_replicas"));
        assertEquals("1", getSettingsResponse.getSetting(indexName, "index.number_of_shards"));
        assertEquals(1, getSettingsResponse.getIndexToSettings().get("get_settings_index").size());
    }

    public void testGetSettingsWithDefaults() throws IOException {
        String indexName = "get_settings_index";
        Settings basicSettings = Settings.builder()
            .put("number_of_shards", 1)
            .put("number_of_replicas", 0)
            .build();
        createIndex(indexName, basicSettings);

        GetSettingsRequest getSettingsRequest = new GetSettingsRequest().indices(indexName).includeDefaults(true);
        GetSettingsResponse getSettingsResponse = execute(getSettingsRequest, highLevelClient().indices()::getSettings,
            highLevelClient().indices()::getSettingsAsync);

        assertNotNull(getSettingsResponse.getSetting(indexName, "index.refresh_interval"));
        assertEquals(IndexSettings.DEFAULT_REFRESH_INTERVAL,
            getSettingsResponse.getIndexToDefaultSettings().get("get_settings_index").getAsTime("index.refresh_interval", null));
        assertEquals("1", getSettingsResponse.getSetting(indexName, "index.number_of_shards"));
    }

    public void testGetSettingsWithDefaultsFiltered() throws IOException {
        String indexName = "get_settings_index";
        Settings basicSettings = Settings.builder()
            .put("number_of_shards", 1)
            .put("number_of_replicas", 0)
            .build();
        createIndex(indexName, basicSettings);

        GetSettingsRequest getSettingsRequest = new GetSettingsRequest()
            .indices(indexName)
            .names("index.refresh_interval")
            .includeDefaults(true);
        GetSettingsResponse getSettingsResponse = execute(getSettingsRequest, highLevelClient().indices()::getSettings,
            highLevelClient().indices()::getSettingsAsync);

        assertNull(getSettingsResponse.getSetting(indexName, "index.number_of_replicas"));
        assertNull(getSettingsResponse.getSetting(indexName, "index.number_of_shards"));
        assertEquals(0, getSettingsResponse.getIndexToSettings().get("get_settings_index").size());
        assertEquals(1, getSettingsResponse.getIndexToDefaultSettings().get("get_settings_index").size());
    }
    public void testPutMapping() throws IOException {
        {
            // Add mappings to index
            String indexName = "mapping_index";
            createIndex(indexName, Settings.EMPTY);

            PutMappingRequest putMappingRequest = new PutMappingRequest(indexName);
            putMappingRequest.type("type_name");
            XContentBuilder mappingBuilder = JsonXContent.contentBuilder();
            mappingBuilder.startObject().startObject("properties").startObject("field");
            mappingBuilder.field("type", "text");
            mappingBuilder.endObject().endObject().endObject();
            putMappingRequest.source(mappingBuilder);

            PutMappingResponse putMappingResponse =
                    execute(putMappingRequest, highLevelClient().indices()::putMapping, highLevelClient().indices()::putMappingAsync,
                            highLevelClient().indices()::putMapping, highLevelClient().indices()::putMappingAsync);
            assertTrue(putMappingResponse.isAcknowledged());

            Map<String, Object> getIndexResponse = getAsMap(indexName);
            assertEquals("text", XContentMapValues.extractValue(indexName + ".mappings.type_name.properties.field.type", getIndexResponse));
        }
    }

    public void testGetMapping() throws IOException {
        String indexName = "test";
        createIndex(indexName, Settings.EMPTY);

        PutMappingRequest putMappingRequest = new PutMappingRequest(indexName);
        putMappingRequest.type("_doc");
        XContentBuilder mappingBuilder = JsonXContent.contentBuilder();
        mappingBuilder.startObject().startObject("properties").startObject("field");
        mappingBuilder.field("type", "text");
        mappingBuilder.endObject().endObject().endObject();
        putMappingRequest.source(mappingBuilder);

        PutMappingResponse putMappingResponse =
            execute(putMappingRequest, highLevelClient().indices()::putMapping, highLevelClient().indices()::putMappingAsync);
        assertTrue(putMappingResponse.isAcknowledged());

        Map<String, Object> getIndexResponse = getAsMap(indexName);
        assertEquals("text", XContentMapValues.extractValue(indexName + ".mappings._doc.properties.field.type", getIndexResponse));

        GetMappingsRequest request = new GetMappingsRequest()
            .indices(indexName)
            .types("_doc");

        GetMappingsResponse getMappingsResponse =
            execute(request, highLevelClient().indices()::getMappings, highLevelClient().indices()::getMappingsAsync);

        Map<String, Object> mappings = getMappingsResponse.getMappings().get(indexName).get("_doc").sourceAsMap();
        Map<String, String> type = new HashMap<>();
        type.put("type", "text");
        Map<String, Object> field = new HashMap<>();
        field.put("field", type);
        Map<String, Object> expected = new HashMap<>();
        expected.put("properties", field);
        assertThat(mappings, equalTo(expected));
    }

    public void testDeleteIndex() throws IOException {
        {
            // Delete index if exists
            String indexName = "test_index";
            createIndex(indexName, Settings.EMPTY);

            DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(indexName);
            DeleteIndexResponse deleteIndexResponse =
                    execute(deleteIndexRequest, highLevelClient().indices()::delete, highLevelClient().indices()::deleteAsync,
                            highLevelClient().indices()::delete, highLevelClient().indices()::deleteAsync);
            assertTrue(deleteIndexResponse.isAcknowledged());

            assertFalse(indexExists(indexName));
        }
        {
            // Return 404 if index doesn't exist
            String nonExistentIndex = "non_existent_index";
            assertFalse(indexExists(nonExistentIndex));

            DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(nonExistentIndex);

            ElasticsearchException exception = expectThrows(ElasticsearchException.class,
                    () -> execute(deleteIndexRequest, highLevelClient().indices()::delete, highLevelClient().indices()::deleteAsync,
                            highLevelClient().indices()::delete, highLevelClient().indices()::deleteAsync));
            assertEquals(RestStatus.NOT_FOUND, exception.status());
        }
    }

    @SuppressWarnings("unchecked")
    public void testUpdateAliases() throws IOException {
        String index = "index";
        String alias = "alias";

        createIndex(index, Settings.EMPTY);
        assertThat(aliasExists(index, alias), equalTo(false));
        assertThat(aliasExists(alias), equalTo(false));

        IndicesAliasesRequest aliasesAddRequest = new IndicesAliasesRequest();
        AliasActions addAction = new AliasActions(AliasActions.Type.ADD).index(index).aliases(alias);
        addAction.routing("routing").searchRouting("search_routing").filter("{\"term\":{\"year\":2016}}");
        aliasesAddRequest.addAliasAction(addAction);
        IndicesAliasesResponse aliasesAddResponse = execute(aliasesAddRequest, highLevelClient().indices()::updateAliases,
                highLevelClient().indices()::updateAliasesAsync, highLevelClient().indices()::updateAliases,
                highLevelClient().indices()::updateAliasesAsync);
        assertTrue(aliasesAddResponse.isAcknowledged());
        assertThat(aliasExists(alias), equalTo(true));
        assertThat(aliasExists(index, alias), equalTo(true));
        Map<String, Object> getAlias = getAlias(index, alias);
        assertThat(getAlias.get("index_routing"), equalTo("routing"));
        assertThat(getAlias.get("search_routing"), equalTo("search_routing"));
        Map<String, Object> filter = (Map<String, Object>) getAlias.get("filter");
        Map<String, Object> term = (Map<String, Object>) filter.get("term");
        assertEquals(2016, term.get("year"));

        String alias2 = "alias2";
        IndicesAliasesRequest aliasesAddRemoveRequest = new IndicesAliasesRequest();
        addAction = new AliasActions(AliasActions.Type.ADD).indices(index).alias(alias2);
        aliasesAddRemoveRequest.addAliasAction(addAction);
        AliasActions removeAction = new AliasActions(AliasActions.Type.REMOVE).index(index).alias(alias);
        aliasesAddRemoveRequest.addAliasAction(removeAction);
        IndicesAliasesResponse aliasesAddRemoveResponse = execute(aliasesAddRemoveRequest, highLevelClient().indices()::updateAliases,
                highLevelClient().indices()::updateAliasesAsync, highLevelClient().indices()::updateAliases,
                highLevelClient().indices()::updateAliasesAsync);
        assertTrue(aliasesAddRemoveResponse.isAcknowledged());
        assertThat(aliasExists(alias), equalTo(false));
        assertThat(aliasExists(alias2), equalTo(true));
        assertThat(aliasExists(index, alias), equalTo(false));
        assertThat(aliasExists(index, alias2), equalTo(true));

        IndicesAliasesRequest aliasesRemoveIndexRequest = new IndicesAliasesRequest();
        AliasActions removeIndexAction = new AliasActions(AliasActions.Type.REMOVE_INDEX).index(index);
        aliasesRemoveIndexRequest.addAliasAction(removeIndexAction);
        IndicesAliasesResponse aliasesRemoveIndexResponse = execute(aliasesRemoveIndexRequest, highLevelClient().indices()::updateAliases,
                highLevelClient().indices()::updateAliasesAsync, highLevelClient().indices()::updateAliases,
                highLevelClient().indices()::updateAliasesAsync);
        assertTrue(aliasesRemoveIndexResponse.isAcknowledged());
        assertThat(aliasExists(alias), equalTo(false));
        assertThat(aliasExists(alias2), equalTo(false));
        assertThat(aliasExists(index, alias), equalTo(false));
        assertThat(aliasExists(index, alias2), equalTo(false));
        assertThat(indexExists(index), equalTo(false));
    }

    public void testAliasesNonExistentIndex() throws IOException {
        String index = "index";
        String alias = "alias";
        String nonExistentIndex = "non_existent_index";

        IndicesAliasesRequest nonExistentIndexRequest = new IndicesAliasesRequest();
        nonExistentIndexRequest.addAliasAction(new AliasActions(AliasActions.Type.ADD).index(nonExistentIndex).alias(alias));
        ElasticsearchException exception = expectThrows(ElasticsearchException.class, () -> execute(nonExistentIndexRequest,
                highLevelClient().indices()::updateAliases, highLevelClient().indices()::updateAliasesAsync,
                highLevelClient().indices()::updateAliases,
                highLevelClient().indices()::updateAliasesAsync));
        assertThat(exception.status(), equalTo(RestStatus.NOT_FOUND));
        assertThat(exception.getMessage(), equalTo("Elasticsearch exception [type=index_not_found_exception, reason=no such index]"));
        assertThat(exception.getMetadata("es.index"), hasItem(nonExistentIndex));

        createIndex(index, Settings.EMPTY);
        IndicesAliasesRequest mixedRequest = new IndicesAliasesRequest();
        mixedRequest.addAliasAction(new AliasActions(AliasActions.Type.ADD).indices(index).aliases(alias));
        mixedRequest.addAliasAction(new AliasActions(AliasActions.Type.REMOVE).indices(nonExistentIndex).alias(alias));
        exception = expectThrows(ElasticsearchStatusException.class,
                () -> execute(mixedRequest, highLevelClient().indices()::updateAliases, highLevelClient().indices()::updateAliasesAsync,
                        highLevelClient().indices()::updateAliases, highLevelClient().indices()::updateAliasesAsync));
        assertThat(exception.status(), equalTo(RestStatus.NOT_FOUND));
        assertThat(exception.getMessage(), equalTo("Elasticsearch exception [type=index_not_found_exception, reason=no such index]"));
        assertThat(exception.getMetadata("es.index"), hasItem(nonExistentIndex));
        assertThat(exception.getMetadata("es.index"), not(hasItem(index)));
        assertThat(aliasExists(index, alias), equalTo(false));
        assertThat(aliasExists(alias), equalTo(false));

        IndicesAliasesRequest removeIndexRequest = new IndicesAliasesRequest();
        removeIndexRequest.addAliasAction(new AliasActions(AliasActions.Type.ADD).index(nonExistentIndex).alias(alias));
        removeIndexRequest.addAliasAction(new AliasActions(AliasActions.Type.REMOVE_INDEX).indices(nonExistentIndex));
        exception = expectThrows(ElasticsearchException.class, () -> execute(removeIndexRequest, highLevelClient().indices()::updateAliases,
                highLevelClient().indices()::updateAliasesAsync, highLevelClient().indices()::updateAliases,
                highLevelClient().indices()::updateAliasesAsync));
        assertThat(exception.status(), equalTo(RestStatus.NOT_FOUND));
        assertThat(exception.getMessage(), equalTo("Elasticsearch exception [type=index_not_found_exception, reason=no such index]"));
        assertThat(exception.getMetadata("es.index"), hasItem(nonExistentIndex));
        assertThat(exception.getMetadata("es.index"), not(hasItem(index)));
        assertThat(aliasExists(index, alias), equalTo(false));
        assertThat(aliasExists(alias), equalTo(false));
    }

    public void testOpenExistingIndex() throws IOException {
        String index = "index";
        createIndex(index, Settings.EMPTY);
        closeIndex(index);
        ResponseException exception = expectThrows(ResponseException.class,
                () -> client().performRequest(HttpGet.METHOD_NAME, index + "/_search"));
        assertThat(exception.getResponse().getStatusLine().getStatusCode(), equalTo(RestStatus.BAD_REQUEST.getStatus()));
        assertThat(exception.getMessage().contains(index), equalTo(true));

        OpenIndexRequest openIndexRequest = new OpenIndexRequest(index);
        OpenIndexResponse openIndexResponse = execute(openIndexRequest, highLevelClient().indices()::open,
                highLevelClient().indices()::openAsync, highLevelClient().indices()::open,
                highLevelClient().indices()::openAsync);
        assertTrue(openIndexResponse.isAcknowledged());

        Response response = client().performRequest(HttpGet.METHOD_NAME, index + "/_search");
        assertThat(response.getStatusLine().getStatusCode(), equalTo(RestStatus.OK.getStatus()));
    }

    public void testOpenNonExistentIndex() throws IOException {
        String nonExistentIndex = "non_existent_index";
        assertFalse(indexExists(nonExistentIndex));

        OpenIndexRequest openIndexRequest = new OpenIndexRequest(nonExistentIndex);
        ElasticsearchException exception = expectThrows(ElasticsearchException.class,
                () -> execute(openIndexRequest, highLevelClient().indices()::open, highLevelClient().indices()::openAsync,
                        highLevelClient().indices()::open, highLevelClient().indices()::openAsync));
        assertEquals(RestStatus.NOT_FOUND, exception.status());

        OpenIndexRequest lenientOpenIndexRequest = new OpenIndexRequest(nonExistentIndex);
        lenientOpenIndexRequest.indicesOptions(IndicesOptions.lenientExpandOpen());
        OpenIndexResponse lenientOpenIndexResponse = execute(lenientOpenIndexRequest, highLevelClient().indices()::open,
                highLevelClient().indices()::openAsync, highLevelClient().indices()::open,
                highLevelClient().indices()::openAsync);
        assertThat(lenientOpenIndexResponse.isAcknowledged(), equalTo(true));

        OpenIndexRequest strictOpenIndexRequest = new OpenIndexRequest(nonExistentIndex);
        strictOpenIndexRequest.indicesOptions(IndicesOptions.strictExpandOpen());
        ElasticsearchException strictException = expectThrows(ElasticsearchException.class,
                () -> execute(openIndexRequest, highLevelClient().indices()::open, highLevelClient().indices()::openAsync,
                        highLevelClient().indices()::open, highLevelClient().indices()::openAsync));
        assertEquals(RestStatus.NOT_FOUND, strictException.status());
    }

    public void testCloseExistingIndex() throws IOException {
        String index = "index";
        createIndex(index, Settings.EMPTY);
        Response response = client().performRequest(HttpGet.METHOD_NAME, index + "/_search");
        assertThat(response.getStatusLine().getStatusCode(), equalTo(RestStatus.OK.getStatus()));

        CloseIndexRequest closeIndexRequest = new CloseIndexRequest(index);
        CloseIndexResponse closeIndexResponse = execute(closeIndexRequest, highLevelClient().indices()::close,
                highLevelClient().indices()::closeAsync, highLevelClient().indices()::close,
                highLevelClient().indices()::closeAsync);
        assertTrue(closeIndexResponse.isAcknowledged());

        ResponseException exception = expectThrows(ResponseException.class,
                () -> client().performRequest(HttpGet.METHOD_NAME, index + "/_search"));
        assertThat(exception.getResponse().getStatusLine().getStatusCode(), equalTo(RestStatus.BAD_REQUEST.getStatus()));
        assertThat(exception.getMessage().contains(index), equalTo(true));
    }

    public void testCloseNonExistentIndex() throws IOException {
        String nonExistentIndex = "non_existent_index";
        assertFalse(indexExists(nonExistentIndex));

        CloseIndexRequest closeIndexRequest = new CloseIndexRequest(nonExistentIndex);
        ElasticsearchException exception = expectThrows(ElasticsearchException.class,
                () -> execute(closeIndexRequest, highLevelClient().indices()::close, highLevelClient().indices()::closeAsync,
                        highLevelClient().indices()::close, highLevelClient().indices()::closeAsync));
        assertEquals(RestStatus.NOT_FOUND, exception.status());
    }

    public void testRefresh() throws IOException {
        {
            String index = "index";
            Settings settings = Settings.builder()
                .put("number_of_shards", 1)
                .put("number_of_replicas", 0)
                .build();
            createIndex(index, settings);
            RefreshRequest refreshRequest = new RefreshRequest(index);
            RefreshResponse refreshResponse =
                execute(refreshRequest, highLevelClient().indices()::refresh, highLevelClient().indices()::refreshAsync,
                        highLevelClient().indices()::refresh, highLevelClient().indices()::refreshAsync);
            assertThat(refreshResponse.getTotalShards(), equalTo(1));
            assertThat(refreshResponse.getSuccessfulShards(), equalTo(1));
            assertThat(refreshResponse.getFailedShards(), equalTo(0));
            assertThat(refreshResponse.getShardFailures(), equalTo(BroadcastResponse.EMPTY));
        }
        {
            String nonExistentIndex = "non_existent_index";
            assertFalse(indexExists(nonExistentIndex));
            RefreshRequest refreshRequest = new RefreshRequest(nonExistentIndex);
            ElasticsearchException exception = expectThrows(ElasticsearchException.class,
                () -> execute(refreshRequest, highLevelClient().indices()::refresh, highLevelClient().indices()::refreshAsync,
                        highLevelClient().indices()::refresh, highLevelClient().indices()::refreshAsync));
            assertEquals(RestStatus.NOT_FOUND, exception.status());
        }
    }

    public void testFlush() throws IOException {
        {
            String index = "index";
            Settings settings = Settings.builder()
                    .put("number_of_shards", 1)
                    .put("number_of_replicas", 0)
                    .build();
            createIndex(index, settings);
            FlushRequest flushRequest = new FlushRequest(index);
            FlushResponse flushResponse =
                    execute(flushRequest, highLevelClient().indices()::flush, highLevelClient().indices()::flushAsync,
                            highLevelClient().indices()::flush, highLevelClient().indices()::flushAsync);
            assertThat(flushResponse.getTotalShards(), equalTo(1));
            assertThat(flushResponse.getSuccessfulShards(), equalTo(1));
            assertThat(flushResponse.getFailedShards(), equalTo(0));
            assertThat(flushResponse.getShardFailures(), equalTo(BroadcastResponse.EMPTY));
        }
        {
            String nonExistentIndex = "non_existent_index";
            assertFalse(indexExists(nonExistentIndex));
            FlushRequest flushRequest = new FlushRequest(nonExistentIndex);
            ElasticsearchException exception = expectThrows(ElasticsearchException.class,
                    () -> execute(flushRequest, highLevelClient().indices()::flush, highLevelClient().indices()::flushAsync,
                            highLevelClient().indices()::flush, highLevelClient().indices()::flushAsync));
            assertEquals(RestStatus.NOT_FOUND, exception.status());
        }
    }

    public void testSyncedFlush() throws IOException {
        {
            String index = "index";
            Settings settings = Settings.builder()
                    .put("number_of_shards", 1)
                    .put("number_of_replicas", 0)
                    .build();
            createIndex(index, settings);
            SyncedFlushRequest syncedFlushRequest = new SyncedFlushRequest(index);
            SyncedFlushResponse flushResponse =
                    execute(syncedFlushRequest, highLevelClient().indices()::flushSynced, highLevelClient().indices()::flushSyncedAsync);
            assertThat(flushResponse.totalShards(), equalTo(1));
            assertThat(flushResponse.successfulShards(), equalTo(1));
            assertThat(flushResponse.failedShards(), equalTo(0));
        }
        {
            String nonExistentIndex = "non_existent_index";
            assertFalse(indexExists(nonExistentIndex));
            SyncedFlushRequest syncedFlushRequest = new SyncedFlushRequest(nonExistentIndex);
            ElasticsearchException exception = expectThrows(
                ElasticsearchException.class,
                () ->
                    execute(
                        syncedFlushRequest,
                        highLevelClient().indices()::flushSynced,
                        highLevelClient().indices()::flushSyncedAsync
                    )
            );
            assertEquals(RestStatus.NOT_FOUND, exception.status());
        }
    }


    public void testClearCache() throws IOException {
        {
            String index = "index";
            Settings settings = Settings.builder()
                    .put("number_of_shards", 1)
                    .put("number_of_replicas", 0)
                    .build();
            createIndex(index, settings);
            ClearIndicesCacheRequest clearCacheRequest = new ClearIndicesCacheRequest(index);
            ClearIndicesCacheResponse clearCacheResponse =
                    execute(clearCacheRequest, highLevelClient().indices()::clearCache, highLevelClient().indices()::clearCacheAsync,
                            highLevelClient().indices()::clearCache, highLevelClient().indices()::clearCacheAsync);
            assertThat(clearCacheResponse.getTotalShards(), equalTo(1));
            assertThat(clearCacheResponse.getSuccessfulShards(), equalTo(1));
            assertThat(clearCacheResponse.getFailedShards(), equalTo(0));
            assertThat(clearCacheResponse.getShardFailures(), equalTo(BroadcastResponse.EMPTY));
        }
        {
            String nonExistentIndex = "non_existent_index";
            assertFalse(indexExists(nonExistentIndex));
            ClearIndicesCacheRequest clearCacheRequest = new ClearIndicesCacheRequest(nonExistentIndex);
            ElasticsearchException exception = expectThrows(ElasticsearchException.class,
                    () -> execute(clearCacheRequest, highLevelClient().indices()::clearCache, highLevelClient().indices()::clearCacheAsync,
                            highLevelClient().indices()::clearCache, highLevelClient().indices()::clearCacheAsync));
            assertEquals(RestStatus.NOT_FOUND, exception.status());
        }
    }

    public void testForceMerge() throws IOException {
        {
            String index = "index";
            Settings settings = Settings.builder()
                .put("number_of_shards", 1)
                .put("number_of_replicas", 0)
                .build();
            createIndex(index, settings);
            ForceMergeRequest forceMergeRequest = new ForceMergeRequest(index);
            ForceMergeResponse forceMergeResponse =
                execute(forceMergeRequest, highLevelClient().indices()::forceMerge, highLevelClient().indices()::forceMergeAsync,
                        highLevelClient().indices()::forceMerge, highLevelClient().indices()::forceMergeAsync);
            assertThat(forceMergeResponse.getTotalShards(), equalTo(1));
            assertThat(forceMergeResponse.getSuccessfulShards(), equalTo(1));
            assertThat(forceMergeResponse.getFailedShards(), equalTo(0));
            assertThat(forceMergeResponse.getShardFailures(), equalTo(BroadcastResponse.EMPTY));
        }
        {
            String nonExistentIndex = "non_existent_index";
            assertFalse(indexExists(nonExistentIndex));
            ForceMergeRequest forceMergeRequest = new ForceMergeRequest(nonExistentIndex);
            ElasticsearchException exception = expectThrows(ElasticsearchException.class,
                () -> execute(forceMergeRequest, highLevelClient().indices()::forceMerge, highLevelClient().indices()::forceMergeAsync,
                        highLevelClient().indices()::forceMerge, highLevelClient().indices()::forceMergeAsync));
            assertEquals(RestStatus.NOT_FOUND, exception.status());
        }
    }

    public void testExistsAlias() throws IOException {
        GetAliasesRequest getAliasesRequest = new GetAliasesRequest("alias");
        assertFalse(execute(getAliasesRequest, highLevelClient().indices()::existsAlias, highLevelClient().indices()::existsAliasAsync,
                highLevelClient().indices()::existsAlias, highLevelClient().indices()::existsAliasAsync));

        createIndex("index", Settings.EMPTY);
        client().performRequest(HttpPut.METHOD_NAME, "/index/_alias/alias");
        assertTrue(execute(getAliasesRequest, highLevelClient().indices()::existsAlias, highLevelClient().indices()::existsAliasAsync,
                highLevelClient().indices()::existsAlias, highLevelClient().indices()::existsAliasAsync));

        GetAliasesRequest getAliasesRequest2 = new GetAliasesRequest();
        getAliasesRequest2.aliases("alias");
        getAliasesRequest2.indices("index");
        assertTrue(execute(getAliasesRequest2, highLevelClient().indices()::existsAlias, highLevelClient().indices()::existsAliasAsync,
                highLevelClient().indices()::existsAlias, highLevelClient().indices()::existsAliasAsync));
        getAliasesRequest2.indices("does_not_exist");
        assertFalse(execute(getAliasesRequest2, highLevelClient().indices()::existsAlias, highLevelClient().indices()::existsAliasAsync,
                highLevelClient().indices()::existsAlias, highLevelClient().indices()::existsAliasAsync));
    }

    @SuppressWarnings("unchecked")
    public void testShrink() throws IOException {
        Map<String, Object> nodes = getAsMap("_nodes");
        String firstNode = ((Map<String, Object>) nodes.get("nodes")).keySet().iterator().next();
        createIndex("source", Settings.builder().put("index.number_of_shards", 4).put("index.number_of_replicas", 0).build());
        updateIndexSettings("source", Settings.builder().put("index.routing.allocation.require._name", firstNode)
                .put("index.blocks.write", true));

        ResizeRequest resizeRequest = new ResizeRequest("target", "source");
        resizeRequest.setResizeType(ResizeType.SHRINK);
        Settings targetSettings =
                Settings.builder()
                        .put("index.number_of_shards", 2)
                        .put("index.number_of_replicas", 0)
                        .putNull("index.routing.allocation.require._name")
                        .build();
        resizeRequest.setTargetIndex(new CreateIndexRequest("target").settings(targetSettings).alias(new Alias("alias")));
        ResizeResponse resizeResponse = execute(resizeRequest, highLevelClient().indices()::shrink,
                highLevelClient().indices()::shrinkAsync, highLevelClient().indices()::shrink, highLevelClient().indices()::shrinkAsync);
        assertTrue(resizeResponse.isAcknowledged());
        assertTrue(resizeResponse.isShardsAcknowledged());
        Map<String, Object> getIndexResponse = getAsMap("target");
        Map<String, Object> indexSettings = (Map<String, Object>)XContentMapValues.extractValue("target.settings.index", getIndexResponse);
        assertNotNull(indexSettings);
        assertEquals("2", indexSettings.get("number_of_shards"));
        assertEquals("0", indexSettings.get("number_of_replicas"));
        Map<String, Object> aliasData = (Map<String, Object>)XContentMapValues.extractValue("target.aliases.alias", getIndexResponse);
        assertNotNull(aliasData);
    }

    @SuppressWarnings("unchecked")
    public void testSplit() throws IOException {
        createIndex("source", Settings.builder().put("index.number_of_shards", 2).put("index.number_of_replicas", 0)
                .put("index.number_of_routing_shards", 4).build());
        updateIndexSettings("source", Settings.builder().put("index.blocks.write", true));

        ResizeRequest resizeRequest = new ResizeRequest("target", "source");
        resizeRequest.setResizeType(ResizeType.SPLIT);
        Settings targetSettings = Settings.builder().put("index.number_of_shards", 4).put("index.number_of_replicas", 0).build();
        resizeRequest.setTargetIndex(new CreateIndexRequest("target").settings(targetSettings).alias(new Alias("alias")));
        ResizeResponse resizeResponse = execute(resizeRequest, highLevelClient().indices()::split, highLevelClient().indices()::splitAsync,
                highLevelClient().indices()::split, highLevelClient().indices()::splitAsync);
        assertTrue(resizeResponse.isAcknowledged());
        assertTrue(resizeResponse.isShardsAcknowledged());
        Map<String, Object> getIndexResponse = getAsMap("target");
        Map<String, Object> indexSettings = (Map<String, Object>)XContentMapValues.extractValue("target.settings.index", getIndexResponse);
        assertNotNull(indexSettings);
        assertEquals("4", indexSettings.get("number_of_shards"));
        assertEquals("0", indexSettings.get("number_of_replicas"));
        Map<String, Object> aliasData = (Map<String, Object>)XContentMapValues.extractValue("target.aliases.alias", getIndexResponse);
        assertNotNull(aliasData);
    }

    public void testRollover() throws IOException {
        highLevelClient().indices().create(new CreateIndexRequest("test").alias(new Alias("alias")), RequestOptions.DEFAULT);
        RolloverRequest rolloverRequest = new RolloverRequest("alias", "test_new");
        rolloverRequest.addMaxIndexDocsCondition(1);

        {
            RolloverResponse rolloverResponse = execute(rolloverRequest, highLevelClient().indices()::rollover,
                    highLevelClient().indices()::rolloverAsync, highLevelClient().indices()::rollover,
                    highLevelClient().indices()::rolloverAsync);
            assertFalse(rolloverResponse.isRolledOver());
            assertFalse(rolloverResponse.isDryRun());
            Map<String, Boolean> conditionStatus = rolloverResponse.getConditionStatus();
            assertEquals(1, conditionStatus.size());
            assertFalse(conditionStatus.get("[max_docs: 1]"));
            assertEquals("test", rolloverResponse.getOldIndex());
            assertEquals("test_new", rolloverResponse.getNewIndex());
        }

        highLevelClient().index(new IndexRequest("test", "type", "1").source("field", "value"), RequestOptions.DEFAULT);
        highLevelClient().index(new IndexRequest("test", "type", "2").source("field", "value")
                .setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL), RequestOptions.DEFAULT);
        //without the refresh the rollover may not happen as the number of docs seen may be off

        {
            rolloverRequest.addMaxIndexAgeCondition(new TimeValue(1));
            rolloverRequest.dryRun(true);
            RolloverResponse rolloverResponse = execute(rolloverRequest, highLevelClient().indices()::rollover,
                    highLevelClient().indices()::rolloverAsync, highLevelClient().indices()::rollover,
                    highLevelClient().indices()::rolloverAsync);
            assertFalse(rolloverResponse.isRolledOver());
            assertTrue(rolloverResponse.isDryRun());
            Map<String, Boolean> conditionStatus = rolloverResponse.getConditionStatus();
            assertEquals(2, conditionStatus.size());
            assertTrue(conditionStatus.get("[max_docs: 1]"));
            assertTrue(conditionStatus.get("[max_age: 1ms]"));
            assertEquals("test", rolloverResponse.getOldIndex());
            assertEquals("test_new", rolloverResponse.getNewIndex());
        }
        {
            rolloverRequest.dryRun(false);
            rolloverRequest.addMaxIndexSizeCondition(new ByteSizeValue(1, ByteSizeUnit.MB));
            RolloverResponse rolloverResponse = execute(rolloverRequest, highLevelClient().indices()::rollover,
                    highLevelClient().indices()::rolloverAsync, highLevelClient().indices()::rollover,
                    highLevelClient().indices()::rolloverAsync);
            assertTrue(rolloverResponse.isRolledOver());
            assertFalse(rolloverResponse.isDryRun());
            Map<String, Boolean> conditionStatus = rolloverResponse.getConditionStatus();
            assertEquals(3, conditionStatus.size());
            assertTrue(conditionStatus.get("[max_docs: 1]"));
            assertTrue(conditionStatus.get("[max_age: 1ms]"));
            assertFalse(conditionStatus.get("[max_size: 1mb]"));
            assertEquals("test", rolloverResponse.getOldIndex());
            assertEquals("test_new", rolloverResponse.getNewIndex());
        }
    }

    public void testIndexPutSettings() throws IOException {

        final Setting<Integer> dynamicSetting = IndexMetaData.INDEX_NUMBER_OF_REPLICAS_SETTING;
        final String dynamicSettingKey = IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
        final int dynamicSettingValue = 0;

        final Setting<String> staticSetting = IndexSettings.INDEX_CHECK_ON_STARTUP;
        final String staticSettingKey = IndexSettings.INDEX_CHECK_ON_STARTUP.getKey();
        final String staticSettingValue = "true";

        final Setting<Integer> unmodifiableSetting = IndexMetaData.INDEX_NUMBER_OF_SHARDS_SETTING;
        final String unmodifiableSettingKey = IndexMetaData.SETTING_NUMBER_OF_SHARDS;
        final int unmodifiableSettingValue = 3;

        String index = "index";
        createIndex(index, Settings.EMPTY);

        assertThat(dynamicSetting.getDefault(Settings.EMPTY), not(dynamicSettingValue));
        UpdateSettingsRequest dynamicSettingRequest = new UpdateSettingsRequest();
        dynamicSettingRequest.settings(Settings.builder().put(dynamicSettingKey, dynamicSettingValue).build());
        UpdateSettingsResponse response = execute(dynamicSettingRequest, highLevelClient().indices()::putSettings,
                highLevelClient().indices()::putSettingsAsync, highLevelClient().indices()::putSettings,
                highLevelClient().indices()::putSettingsAsync);

        assertTrue(response.isAcknowledged());
        Map<String, Object> indexSettingsAsMap = getIndexSettingsAsMap(index);
        assertThat(indexSettingsAsMap.get(dynamicSettingKey), equalTo(String.valueOf(dynamicSettingValue)));

        assertThat(staticSetting.getDefault(Settings.EMPTY), not(staticSettingValue));
        UpdateSettingsRequest staticSettingRequest = new UpdateSettingsRequest();
        staticSettingRequest.settings(Settings.builder().put(staticSettingKey, staticSettingValue).build());
        ElasticsearchException exception = expectThrows(ElasticsearchException.class, () -> execute(staticSettingRequest,
                highLevelClient().indices()::putSettings, highLevelClient().indices()::putSettingsAsync,
                highLevelClient().indices()::putSettings, highLevelClient().indices()::putSettingsAsync));
        assertThat(exception.getMessage(),
                startsWith("Elasticsearch exception [type=illegal_argument_exception, "
                        + "reason=Can't update non dynamic settings [[index.shard.check_on_startup]] for open indices [[index/"));

        indexSettingsAsMap = getIndexSettingsAsMap(index);
        assertNull(indexSettingsAsMap.get(staticSettingKey));

        closeIndex(index);
        response = execute(staticSettingRequest, highLevelClient().indices()::putSettings,
                highLevelClient().indices()::putSettingsAsync, highLevelClient().indices()::putSettings,
                highLevelClient().indices()::putSettingsAsync);
        assertTrue(response.isAcknowledged());
        openIndex(index);
        indexSettingsAsMap = getIndexSettingsAsMap(index);
        assertThat(indexSettingsAsMap.get(staticSettingKey), equalTo(staticSettingValue));

        assertThat(unmodifiableSetting.getDefault(Settings.EMPTY), not(unmodifiableSettingValue));
        UpdateSettingsRequest unmodifiableSettingRequest = new UpdateSettingsRequest();
        unmodifiableSettingRequest.settings(Settings.builder().put(unmodifiableSettingKey, unmodifiableSettingValue).build());
        exception = expectThrows(ElasticsearchException.class, () -> execute(unmodifiableSettingRequest,
                highLevelClient().indices()::putSettings, highLevelClient().indices()::putSettingsAsync,
                highLevelClient().indices()::putSettings, highLevelClient().indices()::putSettingsAsync));
        assertThat(exception.getMessage(), startsWith(
                "Elasticsearch exception [type=illegal_argument_exception, "
                + "reason=Can't update non dynamic settings [[index.number_of_shards]] for open indices [[index/"));
        closeIndex(index);
        exception = expectThrows(ElasticsearchException.class, () -> execute(unmodifiableSettingRequest,
                highLevelClient().indices()::putSettings, highLevelClient().indices()::putSettingsAsync));
        assertThat(exception.getMessage(), startsWith(
                "Elasticsearch exception [type=illegal_argument_exception, "
                + "reason=final index setting [index.number_of_shards], not updateable"));
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> getIndexSettingsAsMap(String index) throws IOException {
        Map<String, Object> indexSettings = getIndexSettings(index);
        return (Map<String, Object>)((Map<String, Object>) indexSettings.get(index)).get("settings");
    }

    public void testIndexPutSettingNonExistent() throws IOException {

        String index = "index";
        UpdateSettingsRequest indexUpdateSettingsRequest = new UpdateSettingsRequest(index);
        String setting = "no_idea_what_you_are_talking_about";
        int value = 10;
        indexUpdateSettingsRequest.settings(Settings.builder().put(setting, value).build());

        ElasticsearchException exception = expectThrows(ElasticsearchException.class, () -> execute(indexUpdateSettingsRequest,
                highLevelClient().indices()::putSettings, highLevelClient().indices()::putSettingsAsync,
                highLevelClient().indices()::putSettings, highLevelClient().indices()::putSettingsAsync));
        assertEquals(RestStatus.NOT_FOUND, exception.status());
        assertThat(exception.getMessage(), equalTo("Elasticsearch exception [type=index_not_found_exception, reason=no such index]"));

        createIndex(index, Settings.EMPTY);
        exception = expectThrows(ElasticsearchException.class, () -> execute(indexUpdateSettingsRequest,
                highLevelClient().indices()::putSettings, highLevelClient().indices()::putSettingsAsync,
                highLevelClient().indices()::putSettings, highLevelClient().indices()::putSettingsAsync));
        assertThat(exception.status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(exception.getMessage(), equalTo(
                "Elasticsearch exception [type=illegal_argument_exception, "
                + "reason=unknown setting [index.no_idea_what_you_are_talking_about] please check that any required plugins are installed, "
                + "or check the breaking changes documentation for removed settings]"));
    }

    @SuppressWarnings("unchecked")
    public void testPutTemplate() throws Exception {
        PutIndexTemplateRequest putTemplateRequest = new PutIndexTemplateRequest()
            .name("my-template")
            .patterns(Arrays.asList("pattern-1", "name-*"))
            .order(10)
            .create(randomBoolean())
            .settings(Settings.builder().put("number_of_shards", "3").put("number_of_replicas", "0"))
            .mapping("doc", "host_name", "type=keyword", "description", "type=text")
            .alias(new Alias("alias-1").indexRouting("abc")).alias(new Alias("{index}-write").searchRouting("xyz"));

        PutIndexTemplateResponse putTemplateResponse = execute(putTemplateRequest,
            highLevelClient().indices()::putTemplate, highLevelClient().indices()::putTemplateAsync);
        assertThat(putTemplateResponse.isAcknowledged(), equalTo(true));

        Map<String, Object> templates = getAsMap("/_template/my-template");
        assertThat(templates.keySet(), hasSize(1));
        assertThat(extractValue("my-template.order", templates), equalTo(10));
        assertThat(extractRawValues("my-template.index_patterns", templates), contains("pattern-1", "name-*"));
        assertThat(extractValue("my-template.settings.index.number_of_shards", templates), equalTo("3"));
        assertThat(extractValue("my-template.settings.index.number_of_replicas", templates), equalTo("0"));
        assertThat(extractValue("my-template.mappings.doc.properties.host_name.type", templates), equalTo("keyword"));
        assertThat(extractValue("my-template.mappings.doc.properties.description.type", templates), equalTo("text"));
        assertThat((Map<String, String>) extractValue("my-template.aliases.alias-1", templates), hasEntry("index_routing", "abc"));
        assertThat((Map<String, String>) extractValue("my-template.aliases.{index}-write", templates), hasEntry("search_routing", "xyz"));
    }

    public void testPutTemplateBadRequests() throws Exception {
        RestHighLevelClient client = highLevelClient();

        // Failed to validate because index patterns are missing
        PutIndexTemplateRequest withoutPattern = new PutIndexTemplateRequest("t1");
        ValidationException withoutPatternError = expectThrows(ValidationException.class,
            () -> execute(withoutPattern, client.indices()::putTemplate, client.indices()::putTemplateAsync));
        assertThat(withoutPatternError.validationErrors(), contains("index patterns are missing"));

        // Create-only specified but an template exists already
        PutIndexTemplateRequest goodTemplate = new PutIndexTemplateRequest("t2").patterns(Arrays.asList("qa-*", "prod-*"));
        assertTrue(execute(goodTemplate, client.indices()::putTemplate, client.indices()::putTemplateAsync).isAcknowledged());
        goodTemplate.create(true);
        ElasticsearchException alreadyExistsError = expectThrows(ElasticsearchException.class,
            () -> execute(goodTemplate, client.indices()::putTemplate, client.indices()::putTemplateAsync));
        assertThat(alreadyExistsError.getDetailedMessage(),
            containsString("[type=illegal_argument_exception, reason=index_template [t2] already exists]"));
        goodTemplate.create(false);
        assertTrue(execute(goodTemplate, client.indices()::putTemplate, client.indices()::putTemplateAsync).isAcknowledged());

        // Rejected due to unknown settings
        PutIndexTemplateRequest unknownSettingTemplate = new PutIndexTemplateRequest("t3")
            .patterns(Collections.singletonList("any"))
            .settings(Settings.builder().put("this-setting-does-not-exist", 100));
        ElasticsearchStatusException unknownSettingError = expectThrows(ElasticsearchStatusException.class,
            () -> execute(unknownSettingTemplate, client.indices()::putTemplate, client.indices()::putTemplateAsync));
        assertThat(unknownSettingError.getDetailedMessage(), containsString("unknown setting [index.this-setting-does-not-exist]"));
    }
}
