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
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesResponse;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.admin.indices.close.CloseIndexResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class IndicesClientIT extends ESRestHighLevelClientTestCase {

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void testCreateIndex() throws IOException {
        {
            // Create index
            String indexName = "plain_index";
            assertFalse(indexExists(indexName));

            CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName);

            CreateIndexResponse createIndexResponse =
                execute(createIndexRequest, highLevelClient().indices()::create, highLevelClient().indices()::createAsync);
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
                execute(createIndexRequest, highLevelClient().indices()::create, highLevelClient().indices()::createAsync);
            assertTrue(createIndexResponse.isAcknowledged());

            Map<String, Object> indexMetaData = getIndexMetadata(indexName);

            Map<String, Object> settingsData = (Map) indexMetaData.get("settings");
            Map<String, Object> indexSettings = (Map) settingsData.get("index");
            assertEquals("2", indexSettings.get("number_of_replicas"));

            Map<String, Object> aliasesData = (Map) indexMetaData.get("aliases");
            Map<String, Object> aliasData = (Map) aliasesData.get("alias_name");
            assertEquals("1", aliasData.get("index_routing"));
            Map<String, Object> filter = (Map) aliasData.get("filter");
            Map<String, Object> term = (Map) filter.get("term");
            assertEquals(2016, term.get("year"));

            Map<String, Object> mappingsData = (Map) indexMetaData.get("mappings");
            Map<String, Object> typeData = (Map) mappingsData.get("type_name");
            Map<String, Object> properties = (Map) typeData.get("properties");
            Map<String, Object> field = (Map) properties.get("field");

            assertEquals("text", field.get("type"));
        }
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void testPutMapping() throws IOException {
        {
            // Add mappings to index
            String indexName = "mapping_index";
            createIndex(indexName);

            PutMappingRequest putMappingRequest = new PutMappingRequest(indexName);
            putMappingRequest.type("type_name");
            XContentBuilder mappingBuilder = JsonXContent.contentBuilder();
            mappingBuilder.startObject().startObject("properties").startObject("field");
            mappingBuilder.field("type", "text");
            mappingBuilder.endObject().endObject().endObject();
            putMappingRequest.source(mappingBuilder);

            PutMappingResponse putMappingResponse =
                execute(putMappingRequest, highLevelClient().indices()::putMapping, highLevelClient().indices()::putMappingAsync);
            assertTrue(putMappingResponse.isAcknowledged());

            Map<String, Object> indexMetaData = getIndexMetadata(indexName);
            Map<String, Object> mappingsData = (Map) indexMetaData.get("mappings");
            Map<String, Object> typeData = (Map) mappingsData.get("type_name");
            Map<String, Object> properties = (Map) typeData.get("properties");
            Map<String, Object> field = (Map) properties.get("field");

            assertEquals("text", field.get("type"));
        }
    }

    public void testDeleteIndex() throws IOException {
        {
            // Delete index if exists
            String indexName = "test_index";
            createIndex(indexName);

            DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(indexName);
            DeleteIndexResponse deleteIndexResponse =
                execute(deleteIndexRequest, highLevelClient().indices()::delete, highLevelClient().indices()::deleteAsync);
            assertTrue(deleteIndexResponse.isAcknowledged());

            assertFalse(indexExists(indexName));
        }
        {
            // Return 404 if index doesn't exist
            String nonExistentIndex = "non_existent_index";
            assertFalse(indexExists(nonExistentIndex));

            DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(nonExistentIndex);

            ElasticsearchException exception = expectThrows(ElasticsearchException.class,
                () -> execute(deleteIndexRequest, highLevelClient().indices()::delete, highLevelClient().indices()::deleteAsync));
            assertEquals(RestStatus.NOT_FOUND, exception.status());
        }
    }

    @SuppressWarnings("unchecked")
    public void testUpdateAliases() throws IOException {
        String index = "index";
        String alias = "alias";

        createIndex(index);
        assertThat(aliasExists(index, alias), equalTo(false));
        assertThat(aliasExists(alias), equalTo(false));

        IndicesAliasesRequest aliasesAddRequest = new IndicesAliasesRequest();
        AliasActions addAction = new AliasActions(AliasActions.Type.ADD).index(index).aliases(alias);
        addAction.routing("routing").searchRouting("search_routing").filter("{\"term\":{\"year\":2016}}");
        aliasesAddRequest.addAliasAction(addAction);
        IndicesAliasesResponse aliasesAddResponse = execute(aliasesAddRequest, highLevelClient().indices()::updateAliases,
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
                highLevelClient().indices()::updateAliases, highLevelClient().indices()::updateAliasesAsync));
        assertThat(exception.status(), equalTo(RestStatus.NOT_FOUND));
        assertThat(exception.getMessage(), equalTo("Elasticsearch exception [type=index_not_found_exception, reason=no such index]"));
        assertThat(exception.getMetadata("es.index"), hasItem(nonExistentIndex));

        createIndex(index);
        IndicesAliasesRequest mixedRequest = new IndicesAliasesRequest();
        mixedRequest.addAliasAction(new AliasActions(AliasActions.Type.ADD).indices(index).aliases(alias));
        mixedRequest.addAliasAction(new AliasActions(AliasActions.Type.REMOVE).indices(nonExistentIndex).alias(alias));
        exception = expectThrows(ElasticsearchStatusException.class,
                () -> execute(mixedRequest, highLevelClient().indices()::updateAliases, highLevelClient().indices()::updateAliasesAsync));
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
        createIndex(index);
        closeIndex(index);
        ResponseException exception = expectThrows(ResponseException.class,
                () -> client().performRequest(HttpGet.METHOD_NAME, index + "/_search"));
        assertThat(exception.getResponse().getStatusLine().getStatusCode(), equalTo(RestStatus.BAD_REQUEST.getStatus()));
        assertThat(exception.getMessage().contains(index), equalTo(true));

        OpenIndexRequest openIndexRequest = new OpenIndexRequest(index);
        OpenIndexResponse openIndexResponse = execute(openIndexRequest, highLevelClient().indices()::open,
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
                () -> execute(openIndexRequest, highLevelClient().indices()::open, highLevelClient().indices()::openAsync));
        assertEquals(RestStatus.NOT_FOUND, exception.status());

        OpenIndexRequest lenientOpenIndexRequest = new OpenIndexRequest(nonExistentIndex);
        lenientOpenIndexRequest.indicesOptions(IndicesOptions.lenientExpandOpen());
        OpenIndexResponse lenientOpenIndexResponse = execute(lenientOpenIndexRequest, highLevelClient().indices()::open,
                highLevelClient().indices()::openAsync);
        assertThat(lenientOpenIndexResponse.isAcknowledged(), equalTo(true));

        OpenIndexRequest strictOpenIndexRequest = new OpenIndexRequest(nonExistentIndex);
        strictOpenIndexRequest.indicesOptions(IndicesOptions.strictExpandOpen());
        ElasticsearchException strictException = expectThrows(ElasticsearchException.class,
                () -> execute(openIndexRequest, highLevelClient().indices()::open, highLevelClient().indices()::openAsync));
        assertEquals(RestStatus.NOT_FOUND, strictException.status());
    }

    public void testCloseExistingIndex() throws IOException {
        String index = "index";
        createIndex(index);
        Response response = client().performRequest(HttpGet.METHOD_NAME, index + "/_search");
        assertThat(response.getStatusLine().getStatusCode(), equalTo(RestStatus.OK.getStatus()));

        CloseIndexRequest closeIndexRequest = new CloseIndexRequest(index);
        CloseIndexResponse closeIndexResponse = execute(closeIndexRequest, highLevelClient().indices()::close,
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
                () -> execute(closeIndexRequest, highLevelClient().indices()::close, highLevelClient().indices()::closeAsync));
        assertEquals(RestStatus.NOT_FOUND, exception.status());
    }

    private static void createIndex(String index) throws IOException {
        Response response = client().performRequest(HttpPut.METHOD_NAME, index);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(RestStatus.OK.getStatus()));
    }

    private static boolean indexExists(String index) throws IOException {
        Response response = client().performRequest(HttpHead.METHOD_NAME, index);
        return RestStatus.OK.getStatus() == response.getStatusLine().getStatusCode();
    }

    private static void closeIndex(String index) throws IOException {
        Response response = client().performRequest(HttpPost.METHOD_NAME, index + "/_close");
        assertThat(response.getStatusLine().getStatusCode(), equalTo(RestStatus.OK.getStatus()));
    }

    private static boolean aliasExists(String alias) throws IOException {
        Response response = client().performRequest(HttpHead.METHOD_NAME, "/_alias/" + alias);
        return RestStatus.OK.getStatus() == response.getStatusLine().getStatusCode();
    }

    private static boolean aliasExists(String index, String alias) throws IOException {
        Response response = client().performRequest(HttpHead.METHOD_NAME, "/" + index + "/_alias/" + alias);
        return RestStatus.OK.getStatus() == response.getStatusLine().getStatusCode();
    }

    public void testExistsAlias() throws IOException {
        GetAliasesRequest getAliasesRequest = new GetAliasesRequest("alias");
        assertFalse(execute(getAliasesRequest, highLevelClient().indices()::existsAlias, highLevelClient().indices()::existsAliasAsync));

        createIndex("index");
        client().performRequest(HttpPut.METHOD_NAME, "/index/_alias/alias");
        assertTrue(execute(getAliasesRequest, highLevelClient().indices()::existsAlias, highLevelClient().indices()::existsAliasAsync));

        GetAliasesRequest getAliasesRequest2 = new GetAliasesRequest();
        getAliasesRequest2.aliases("alias");
        getAliasesRequest2.indices("index");
        assertTrue(execute(getAliasesRequest2, highLevelClient().indices()::existsAlias, highLevelClient().indices()::existsAliasAsync));
        getAliasesRequest2.indices("does_not_exist");
        assertFalse(execute(getAliasesRequest2, highLevelClient().indices()::existsAlias, highLevelClient().indices()::existsAliasAsync));
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private Map<String, Object> getIndexMetadata(String index) throws IOException {
        Response response = client().performRequest(HttpGet.METHOD_NAME, index);

        XContentType entityContentType = XContentType.fromMediaTypeOrFormat(response.getEntity().getContentType().getValue());
        Map<String, Object> responseEntity = XContentHelper.convertToMap(entityContentType.xContent(), response.getEntity().getContent(),
            false);

        Map<String, Object> indexMetaData = (Map) responseEntity.get(index);
        assertNotNull(indexMetaData);

        return indexMetaData;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static Map<String, Object> getAlias(final String index, final String alias) throws IOException {
        String endpoint = "/_alias";
        if (false == Strings.isEmpty(index)) {
            endpoint = index + endpoint;
        }
        if (false == Strings.isEmpty(alias)) {
            endpoint = endpoint + "/" + alias;
        }
        Map<String, Object> performGet = performGet(endpoint);
        return (Map) ((Map) ((Map) performGet.get(index)).get("aliases")).get(alias);
    }

    private static Map<String, Object> performGet(final String endpoint) throws IOException {
        Response response = client().performRequest(HttpGet.METHOD_NAME, endpoint);
        XContentType entityContentType = XContentType.fromMediaTypeOrFormat(response.getEntity().getContentType().getValue());
        Map<String, Object> responseEntity = XContentHelper.convertToMap(entityContentType.xContent(), response.getEntity().getContent(),
                false);
        assertNotNull(responseEntity);
        return responseEntity;
    }
}
