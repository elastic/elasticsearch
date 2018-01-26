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

package org.elasticsearch.client.documentation;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.admin.indices.close.CloseIndexResponse;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexResponse;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.ESRestHighLevelClientTestCase;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;

/**
 * This class is used to generate the Java Indices API documentation.
 * You need to wrap your code between two tags like:
 * // tag::example[]
 * // end::example[]
 *
 * Where example is your tag name.
 *
 * Then in the documentation, you can extract what is between tag and end tags with
 * ["source","java",subs="attributes,callouts,macros"]
 * --------------------------------------------------
 * include-tagged::{doc-tests}/CRUDDocumentationIT.java[example]
 * --------------------------------------------------
 */
public class IndicesClientDocumentationIT extends ESRestHighLevelClientTestCase {

    public void testDeleteIndex() throws IOException {
        RestHighLevelClient client = highLevelClient();

        {
            CreateIndexResponse createIndexResponse = client.indices().create(new CreateIndexRequest("posts"));
            assertTrue(createIndexResponse.isAcknowledged());
        }

        {
            // tag::delete-index-request
            DeleteIndexRequest request = new DeleteIndexRequest("posts"); // <1>
            // end::delete-index-request

            // tag::delete-index-request-timeout
            request.timeout(TimeValue.timeValueMinutes(2)); // <1>
            request.timeout("2m"); // <2>
            // end::delete-index-request-timeout
            // tag::delete-index-request-masterTimeout
            request.masterNodeTimeout(TimeValue.timeValueMinutes(1)); // <1>
            request.masterNodeTimeout("1m"); // <2>
            // end::delete-index-request-masterTimeout
            // tag::delete-index-request-indicesOptions
            request.indicesOptions(IndicesOptions.lenientExpandOpen()); // <1>
            // end::delete-index-request-indicesOptions

            // tag::delete-index-execute
            DeleteIndexResponse deleteIndexResponse = client.indices().delete(request);
            // end::delete-index-execute

            // tag::delete-index-response
            boolean acknowledged = deleteIndexResponse.isAcknowledged(); // <1>
            // end::delete-index-response
            assertTrue(acknowledged);
        }

        {
            // tag::delete-index-notfound
            try {
                DeleteIndexRequest request = new DeleteIndexRequest("does_not_exist");
                client.indices().delete(request);
            } catch (ElasticsearchException exception) {
                if (exception.status() == RestStatus.NOT_FOUND) {
                    // <1>
                }
            }
            // end::delete-index-notfound
        }
    }

    public void testDeleteIndexAsync() throws Exception {
        final RestHighLevelClient client = highLevelClient();

        {
            CreateIndexResponse createIndexResponse = client.indices().create(new CreateIndexRequest("posts"));
            assertTrue(createIndexResponse.isAcknowledged());
        }

        {
            DeleteIndexRequest request = new DeleteIndexRequest("posts");

            // tag::delete-index-execute-async
            client.indices().deleteAsync(request, new ActionListener<DeleteIndexResponse>() {
                @Override
                public void onResponse(DeleteIndexResponse deleteIndexResponse) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            });
            // end::delete-index-execute-async

            assertBusy(() -> {
                // TODO Use Indices Exist API instead once it exists
                Response response = client.getLowLevelClient().performRequest("HEAD", "posts");
                assertTrue(RestStatus.NOT_FOUND.getStatus() == response.getStatusLine().getStatusCode());
            });
        }
    }

    public void testCreateIndex() throws IOException {
        RestHighLevelClient client = highLevelClient();

        {
            // tag::create-index-request
            CreateIndexRequest request = new CreateIndexRequest("twitter"); // <1>
            // end::create-index-request

            // tag::create-index-request-settings
            request.settings(Settings.builder() // <1>
                .put("index.number_of_shards", 3)
                .put("index.number_of_replicas", 2)
            );
            // end::create-index-request-settings

            // tag::create-index-request-mappings
            request.mapping("tweet", // <1>
                "{\n" +
                "  \"tweet\": {\n" +
                "    \"properties\": {\n" +
                "      \"message\": {\n" +
                "        \"type\": \"text\"\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}", // <2>
                XContentType.JSON);
            // end::create-index-request-mappings

            // tag::create-index-request-aliases
            request.alias(
                new Alias("twitter_alias")  // <1>
            );
            // end::create-index-request-aliases

            // tag::create-index-request-timeout
            request.timeout(TimeValue.timeValueMinutes(2)); // <1>
            request.timeout("2m"); // <2>
            // end::create-index-request-timeout
            // tag::create-index-request-masterTimeout
            request.masterNodeTimeout(TimeValue.timeValueMinutes(1)); // <1>
            request.masterNodeTimeout("1m"); // <2>
            // end::create-index-request-masterTimeout
            // tag::create-index-request-waitForActiveShards
            request.waitForActiveShards(2); // <1>
            request.waitForActiveShards(ActiveShardCount.DEFAULT); // <2>
            // end::create-index-request-waitForActiveShards

            // tag::create-index-execute
            CreateIndexResponse createIndexResponse = client.indices().create(request);
            // end::create-index-execute

            // tag::create-index-response
            boolean acknowledged = createIndexResponse.isAcknowledged(); // <1>
            boolean shardsAcknowledged = createIndexResponse.isShardsAcknowledged(); // <2>
            // end::create-index-response
            assertTrue(acknowledged);
            assertTrue(shardsAcknowledged);
        }
    }

    public void testCreateIndexAsync() throws Exception {
        final RestHighLevelClient client = highLevelClient();

        {
            CreateIndexRequest request = new CreateIndexRequest("twitter");
            // tag::create-index-execute-async
            client.indices().createAsync(request, new ActionListener<CreateIndexResponse>() {
                @Override
                public void onResponse(CreateIndexResponse createIndexResponse) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            });
            // end::create-index-execute-async

            assertBusy(() -> {
                // TODO Use Indices Exist API instead once it exists
                Response response = client.getLowLevelClient().performRequest("HEAD", "twitter");
                assertTrue(RestStatus.OK.getStatus() == response.getStatusLine().getStatusCode());
            });
        }
    }

    public void testPutMapping() throws IOException {
        RestHighLevelClient client = highLevelClient();

        {
            CreateIndexResponse createIndexResponse = client.indices().create(new CreateIndexRequest("twitter"));
            assertTrue(createIndexResponse.isAcknowledged());
        }

        {
            // tag::put-mapping-request
            PutMappingRequest request = new PutMappingRequest("twitter"); // <1>
            request.type("tweet"); // <2>
            // end::put-mapping-request

            // tag::put-mapping-request-source
            request.source(
                "{\n" +
                "  \"tweet\": {\n" +
                "    \"properties\": {\n" +
                "      \"message\": {\n" +
                "        \"type\": \"text\"\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}", // <1>
                XContentType.JSON);
            // end::put-mapping-request-source

            // tag::put-mapping-request-timeout
            request.timeout(TimeValue.timeValueMinutes(2)); // <1>
            request.timeout("2m"); // <2>
            // end::put-mapping-request-timeout
            // tag::put-mapping-request-masterTimeout
            request.masterNodeTimeout(TimeValue.timeValueMinutes(1)); // <1>
            request.masterNodeTimeout("1m"); // <2>
            // end::put-mapping-request-masterTimeout

            // tag::put-mapping-execute
            PutMappingResponse putMappingResponse = client.indices().putMapping(request);
            // end::put-mapping-execute

            // tag::put-mapping-response
            boolean acknowledged = putMappingResponse.isAcknowledged(); // <1>
            // end::put-mapping-response
            assertTrue(acknowledged);
        }
    }

    public void testPutMappingAsync() throws Exception {
        final RestHighLevelClient client = highLevelClient();

        {
            CreateIndexResponse createIndexResponse = client.indices().create(new CreateIndexRequest("twitter"));
            assertTrue(createIndexResponse.isAcknowledged());
        }

        {
            PutMappingRequest request = new PutMappingRequest("twitter").type("tweet");
            // tag::put-mapping-execute-async
            client.indices().putMappingAsync(request, new ActionListener<PutMappingResponse>() {
                @Override
                public void onResponse(PutMappingResponse putMappingResponse) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            });
            // end::put-mapping-execute-async
        }
    }

    public void testOpenIndex() throws Exception {
        RestHighLevelClient client = highLevelClient();

        {
            CreateIndexResponse createIndexResponse = client.indices().create(new CreateIndexRequest("index"));
            assertTrue(createIndexResponse.isAcknowledged());
        }

        {
            // tag::open-index-request
            OpenIndexRequest request = new OpenIndexRequest("index"); // <1>
            // end::open-index-request

            // tag::open-index-request-timeout
            request.timeout(TimeValue.timeValueMinutes(2)); // <1>
            request.timeout("2m"); // <2>
            // end::open-index-request-timeout
            // tag::open-index-request-masterTimeout
            request.masterNodeTimeout(TimeValue.timeValueMinutes(1)); // <1>
            request.masterNodeTimeout("1m"); // <2>
            // end::open-index-request-masterTimeout
            // tag::open-index-request-waitForActiveShards
            request.waitForActiveShards(2); // <1>
            request.waitForActiveShards(ActiveShardCount.DEFAULT); // <2>
            // end::open-index-request-waitForActiveShards


            // tag::open-index-request-indicesOptions
            request.indicesOptions(IndicesOptions.strictExpandOpen()); // <1>
            // end::open-index-request-indicesOptions

            // tag::open-index-execute
            OpenIndexResponse openIndexResponse = client.indices().open(request);
            // end::open-index-execute

            // tag::open-index-response
            boolean acknowledged = openIndexResponse.isAcknowledged(); // <1>
            boolean shardsAcked = openIndexResponse.isShardsAcknowledged(); // <2>
            // end::open-index-response
            assertTrue(acknowledged);
            assertTrue(shardsAcked);

            // tag::open-index-execute-async
            client.indices().openAsync(request, new ActionListener<OpenIndexResponse>() {
                @Override
                public void onResponse(OpenIndexResponse openIndexResponse) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            });
            // end::open-index-execute-async
        }

        {
            // tag::open-index-notfound
            try {
                OpenIndexRequest request = new OpenIndexRequest("does_not_exist");
                client.indices().open(request);
            } catch (ElasticsearchException exception) {
                if (exception.status() == RestStatus.BAD_REQUEST) {
                    // <1>
                }
            }
            // end::open-index-notfound
        }
    }

    public void testCloseIndex() throws Exception {
        RestHighLevelClient client = highLevelClient();

        {
            CreateIndexResponse createIndexResponse = client.indices().create(new CreateIndexRequest("index"));
            assertTrue(createIndexResponse.isAcknowledged());
        }

        {
            // tag::close-index-request
            CloseIndexRequest request = new CloseIndexRequest("index"); // <1>
            // end::close-index-request

            // tag::close-index-request-timeout
            request.timeout(TimeValue.timeValueMinutes(2)); // <1>
            request.timeout("2m"); // <2>
            // end::close-index-request-timeout
            // tag::close-index-request-masterTimeout
            request.masterNodeTimeout(TimeValue.timeValueMinutes(1)); // <1>
            request.masterNodeTimeout("1m"); // <2>
            // end::close-index-request-masterTimeout

            // tag::close-index-request-indicesOptions
            request.indicesOptions(IndicesOptions.lenientExpandOpen()); // <1>
            // end::close-index-request-indicesOptions

            // tag::close-index-execute
            CloseIndexResponse closeIndexResponse = client.indices().close(request);
            // end::close-index-execute

            // tag::close-index-response
            boolean acknowledged = closeIndexResponse.isAcknowledged(); // <1>
            // end::close-index-response
            assertTrue(acknowledged);

            // tag::close-index-execute-async
            client.indices().closeAsync(request, new ActionListener<CloseIndexResponse>() {
                @Override
                public void onResponse(CloseIndexResponse closeIndexResponse) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            });
            // end::close-index-execute-async

        }
    }

    public void testExistsAlias() throws Exception {
        RestHighLevelClient client = highLevelClient();

        {
            CreateIndexResponse createIndexResponse = client.indices().create(new CreateIndexRequest("index")
                    .alias(new Alias("alias")));
            assertTrue(createIndexResponse.isAcknowledged());
        }

        {
            // tag::exists-alias-request
            GetAliasesRequest request = new GetAliasesRequest();
            GetAliasesRequest requestWithAlias = new GetAliasesRequest("alias1");
            GetAliasesRequest requestWithAliases = new GetAliasesRequest(new String[]{"alias1", "alias2"});
            // end::exists-alias-request

            // tag::exists-alias-request-alias
            request.aliases("alias"); // <1>
            // end::exists-alias-request-alias
            // tag::exists-alias-request-indices
            request.indices("index"); // <1>
            // end::exists-alias-request-indices

            // tag::exists-alias-request-indicesOptions
            request.indicesOptions(IndicesOptions.lenientExpandOpen()); // <1>
            // end::exists-alias-request-indicesOptions

            // tag::exists-alias-request-local
            request.local(true); // <1>
            // end::exists-alias-request-local

            // tag::exists-alias-execute
            boolean exists = client.indices().existsAlias(request);
            // end::exists-alias-execute
            assertTrue(exists);

            // tag::exists-alias-execute-async
            client.indices().existsAliasAsync(request, new ActionListener<Boolean>() {
                @Override
                public void onResponse(Boolean exists) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            });
            // end::exists-alias-execute-async
        }
    }

    public void testIndicesAliases() throws IOException {
        RestHighLevelClient client = highLevelClient();

        {
            CreateIndexResponse createIndexResponse = client.indices().create(new CreateIndexRequest("index1"));
            assertTrue(createIndexResponse.isAcknowledged());
            createIndexResponse = client.indices().create(new CreateIndexRequest("index2"));
            assertTrue(createIndexResponse.isAcknowledged());
            createIndexResponse = client.indices().create(new CreateIndexRequest("index3"));
            assertTrue(createIndexResponse.isAcknowledged());
            createIndexResponse = client.indices().create(new CreateIndexRequest("index4"));
            assertTrue(createIndexResponse.isAcknowledged());
        }

        {
            // tag::update-aliases-request
            IndicesAliasesRequest request = new IndicesAliasesRequest(); // <1>
            AliasActions aliasAction = new AliasActions(AliasActions.Type.ADD).index("index1").alias("alias1"); // <2>
            request.addAliasAction(aliasAction); // <3>
            // end::update-aliases-request

            // tag::update-aliases-request2
            AliasActions addIndexAction = new AliasActions(AliasActions.Type.ADD).index("index1").alias("alias1")
                    .filter("{\"term\":{\"year\":2016}}"); // <1>
            AliasActions addIndicesAction = new AliasActions(AliasActions.Type.ADD).indices("index1", "index2").alias("alias2")
                    .routing("1"); // <2>
            AliasActions removeAction = new AliasActions(AliasActions.Type.REMOVE).index("index3").alias("alias3"); // <3>
            AliasActions removeIndexAction = new AliasActions(AliasActions.Type.REMOVE_INDEX).index("index4"); // <4>
            // end::update-aliases-request2

            // tag::update-aliases-request-timeout
            request.timeout(TimeValue.timeValueMinutes(2)); // <1>
            request.timeout("2m"); // <2>
            // end::update-aliases-request-timeout
            // tag::update-aliases-request-masterTimeout
            request.masterNodeTimeout(TimeValue.timeValueMinutes(1)); // <1>
            request.masterNodeTimeout("1m"); // <2>
            // end::update-aliases-request-masterTimeout

            // tag::update-aliases-execute
            IndicesAliasesResponse indicesAliasesResponse = client.indices().updateAliases(request);
            // end::update-aliases-execute

            // tag::update-aliases-response
            boolean acknowledged = indicesAliasesResponse.isAcknowledged(); // <1>
            // end::update-aliases-response
            assertTrue(acknowledged);

            // tag::update-aliases-execute-async
            client.indices().updateAliasesAsync(request, new ActionListener<IndicesAliasesResponse>() {
                @Override
                public void onResponse(IndicesAliasesResponse indciesAliasesResponse) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            });
            // end::update-aliases-execute-async
        }
    }    
}
