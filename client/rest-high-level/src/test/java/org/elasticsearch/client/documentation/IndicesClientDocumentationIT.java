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
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.cache.clear.ClearIndicesCacheRequest;
import org.elasticsearch.action.admin.indices.cache.clear.ClearIndicesCacheResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.admin.indices.flush.FlushResponse;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeResponse;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.admin.indices.shrink.ResizeRequest;
import org.elasticsearch.action.admin.indices.shrink.ResizeResponse;
import org.elasticsearch.action.admin.indices.shrink.ResizeType;
import org.elasticsearch.action.admin.indices.template.delete.DeleteIndexTemplateRequest;
import org.elasticsearch.action.admin.indices.validate.query.QueryExplanation;
import org.elasticsearch.action.admin.indices.validate.query.ValidateQueryRequest;
import org.elasticsearch.action.admin.indices.validate.query.ValidateQueryResponse;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.ESRestHighLevelClientTestCase;
import org.elasticsearch.client.GetAliasesResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.BroadcastResponse.Shards;
import org.elasticsearch.client.core.ShardsAcknowledgedResponse;
import org.elasticsearch.client.indices.AnalyzeRequest;
import org.elasticsearch.client.indices.AnalyzeResponse;
import org.elasticsearch.client.indices.CloseIndexRequest;
import org.elasticsearch.client.indices.CloseIndexResponse;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.DeleteAliasRequest;
import org.elasticsearch.client.indices.DeleteIndexTemplateV2Request;
import org.elasticsearch.client.indices.DetailAnalyzeResponse;
import org.elasticsearch.client.indices.FreezeIndexRequest;
import org.elasticsearch.client.indices.GetFieldMappingsRequest;
import org.elasticsearch.client.indices.GetFieldMappingsResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.elasticsearch.client.indices.GetIndexTemplateV2Request;
import org.elasticsearch.client.indices.GetIndexTemplatesRequest;
import org.elasticsearch.client.indices.GetIndexTemplatesResponse;
import org.elasticsearch.client.indices.GetIndexTemplatesV2Response;
import org.elasticsearch.client.indices.GetMappingsRequest;
import org.elasticsearch.client.indices.GetMappingsResponse;
import org.elasticsearch.client.indices.IndexTemplateMetadata;
import org.elasticsearch.client.indices.IndexTemplatesExistRequest;
import org.elasticsearch.client.indices.PutComponentTemplateRequest;
import org.elasticsearch.client.indices.PutIndexTemplateRequest;
import org.elasticsearch.client.indices.PutIndexTemplateV2Request;
import org.elasticsearch.client.indices.PutMappingRequest;
import org.elasticsearch.client.indices.ReloadAnalyzersRequest;
import org.elasticsearch.client.indices.ReloadAnalyzersResponse;
import org.elasticsearch.client.indices.ReloadAnalyzersResponse.ReloadDetails;
import org.elasticsearch.client.indices.SimulateIndexTemplateRequest;
import org.elasticsearch.client.indices.SimulateIndexTemplateResponse;
import org.elasticsearch.client.indices.UnfreezeIndexRequest;
import org.elasticsearch.client.indices.rollover.RolloverRequest;
import org.elasticsearch.client.indices.rollover.RolloverResponse;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.ComponentTemplate;
import org.elasticsearch.cluster.metadata.IndexTemplateV2;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 * This class is used to generate the Java Indices API documentation.
 * You need to wrap your code between two tags like:
 * // tag::example
 * // end::example
 *
 * Where example is your tag name.
 *
 * Then in the documentation, you can extract what is between tag and end tags with
 * ["source","java",subs="attributes,callouts,macros"]
 * --------------------------------------------------
 * include-tagged::{doc-tests}/IndicesClientDocumentationIT.java[example]
 * --------------------------------------------------
 *
 * The column width of the code block is 84. If the code contains a line longer
 * than 84, the line will be cut and a horizontal scroll bar will be displayed.
 * (the code indentation of the tag is not included in the width)
 */
public class IndicesClientDocumentationIT extends ESRestHighLevelClientTestCase {

    public void testIndicesExist() throws IOException {
        RestHighLevelClient client = highLevelClient();

        {
            CreateIndexResponse createIndexResponse = client.indices().create(new CreateIndexRequest("twitter"),
                RequestOptions.DEFAULT);
            assertTrue(createIndexResponse.isAcknowledged());
        }

        {
            // tag::indices-exists-request
            GetIndexRequest request = new GetIndexRequest("twitter"); // <1>
            // end::indices-exists-request

            IndicesOptions indicesOptions = IndicesOptions.strictExpand();
            // tag::indices-exists-request-optionals
            request.local(false); // <1>
            request.humanReadable(true); // <2>
            request.includeDefaults(false); // <3>
            request.indicesOptions(indicesOptions); // <4>
            // end::indices-exists-request-optionals

            // tag::indices-exists-execute
            boolean exists = client.indices().exists(request, RequestOptions.DEFAULT);
            // end::indices-exists-execute
            assertTrue(exists);
        }
    }

    public void testIndicesExistAsync() throws Exception {
        RestHighLevelClient client = highLevelClient();

        {
            CreateIndexResponse createIndexResponse = client.indices().create(new CreateIndexRequest("twitter"), RequestOptions.DEFAULT);
            assertTrue(createIndexResponse.isAcknowledged());
        }

        {
            GetIndexRequest request = new GetIndexRequest("twitter");

            // tag::indices-exists-execute-listener
            ActionListener<Boolean> listener = new ActionListener<Boolean>() {
                @Override
                public void onResponse(Boolean exists) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            // end::indices-exists-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::indices-exists-execute-async
            client.indices().existsAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::indices-exists-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }
    public void testDeleteIndex() throws IOException {
        RestHighLevelClient client = highLevelClient();

        {
            CreateIndexResponse createIndexResponse = client.indices().create(new CreateIndexRequest("posts"), RequestOptions.DEFAULT);
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
            AcknowledgedResponse deleteIndexResponse = client.indices().delete(request, RequestOptions.DEFAULT);
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
                client.indices().delete(request, RequestOptions.DEFAULT);
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
            CreateIndexResponse createIndexResponse = client.indices().create(new CreateIndexRequest("posts"), RequestOptions.DEFAULT);
            assertTrue(createIndexResponse.isAcknowledged());
        }

        {
            DeleteIndexRequest request = new DeleteIndexRequest("posts");

            // tag::delete-index-execute-listener
            ActionListener<AcknowledgedResponse> listener =
                    new ActionListener<AcknowledgedResponse>() {
                @Override
                public void onResponse(AcknowledgedResponse deleteIndexResponse) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            // end::delete-index-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::delete-index-execute-async
            client.indices().deleteAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::delete-index-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
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

            {
                // tag::create-index-request-mappings
                request.mapping(// <1>
                        "{\n" +
                        "  \"properties\": {\n" +
                        "    \"message\": {\n" +
                        "      \"type\": \"text\"\n" +
                        "    }\n" +
                        "  }\n" +
                        "}", // <2>
                        XContentType.JSON);
                // end::create-index-request-mappings
                CreateIndexResponse createIndexResponse = client.indices().create(request, RequestOptions.DEFAULT);
                assertTrue(createIndexResponse.isAcknowledged());
            }

            {
                request = new CreateIndexRequest("twitter2");
                //tag::create-index-mappings-map
                Map<String, Object> message = new HashMap<>();
                message.put("type", "text");
                Map<String, Object> properties = new HashMap<>();
                properties.put("message", message);
                Map<String, Object> mapping = new HashMap<>();
                mapping.put("properties", properties);
                request.mapping(mapping); // <1>
                //end::create-index-mappings-map
                CreateIndexResponse createIndexResponse = client.indices().create(request, RequestOptions.DEFAULT);
                assertTrue(createIndexResponse.isAcknowledged());
            }
            {
                request = new CreateIndexRequest("twitter3");
                //tag::create-index-mappings-xcontent
                XContentBuilder builder = XContentFactory.jsonBuilder();
                builder.startObject();
                {
                    builder.startObject("properties");
                    {
                        builder.startObject("message");
                        {
                            builder.field("type", "text");
                        }
                        builder.endObject();
                    }
                    builder.endObject();
                }
                builder.endObject();
                request.mapping(builder); // <1>
                //end::create-index-mappings-xcontent
                CreateIndexResponse createIndexResponse = client.indices().create(request, RequestOptions.DEFAULT);
                assertTrue(createIndexResponse.isAcknowledged());
            }

            request = new CreateIndexRequest("twitter5");
            // tag::create-index-request-aliases
            request.alias(new Alias("twitter_alias").filter(QueryBuilders.termQuery("user", "kimchy")));  // <1>
            // end::create-index-request-aliases

            // tag::create-index-request-timeout
            request.setTimeout(TimeValue.timeValueMinutes(2)); // <1>
            // end::create-index-request-timeout
            // tag::create-index-request-masterTimeout
            request.setMasterTimeout(TimeValue.timeValueMinutes(1)); // <1>
            // end::create-index-request-masterTimeout
            // tag::create-index-request-waitForActiveShards
            request.waitForActiveShards(ActiveShardCount.from(2)); // <1>
            request.waitForActiveShards(ActiveShardCount.DEFAULT); // <2>
            // end::create-index-request-waitForActiveShards
            {
                CreateIndexResponse createIndexResponse = client.indices().create(request, RequestOptions.DEFAULT);
                assertTrue(createIndexResponse.isAcknowledged());
            }

            request = new CreateIndexRequest("twitter6");
            // tag::create-index-whole-source
            request.source("{\n" +
                    "    \"settings\" : {\n" +
                    "        \"number_of_shards\" : 1,\n" +
                    "        \"number_of_replicas\" : 0\n" +
                    "    },\n" +
                    "    \"mappings\" : {\n" +
                    "        \"properties\" : {\n" +
                    "            \"message\" : { \"type\" : \"text\" }\n" +
                    "        }\n" +
                    "    },\n" +
                    "    \"aliases\" : {\n" +
                    "        \"twitter_alias\" : {}\n" +
                    "    }\n" +
                    "}", XContentType.JSON); // <1>
            // end::create-index-whole-source

            // tag::create-index-execute
            CreateIndexResponse createIndexResponse = client.indices().create(request, RequestOptions.DEFAULT);
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

            // tag::create-index-execute-listener
            ActionListener<CreateIndexResponse> listener =
                    new ActionListener<CreateIndexResponse>() {

                @Override
                public void onResponse(CreateIndexResponse createIndexResponse) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            // end::create-index-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::create-index-execute-async
            client.indices().createAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::create-index-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testPutMapping() throws IOException {
        RestHighLevelClient client = highLevelClient();

        {
            CreateIndexResponse createIndexResponse = client.indices().create(new CreateIndexRequest("twitter"), RequestOptions.DEFAULT);
            assertTrue(createIndexResponse.isAcknowledged());
        }

        {
            // tag::put-mapping-request
            PutMappingRequest request = new PutMappingRequest("twitter"); // <1>
            // end::put-mapping-request

            {
                // tag::put-mapping-request-source
                request.source(
                    "{\n" +
                    "  \"properties\": {\n" +
                    "    \"message\": {\n" +
                    "      \"type\": \"text\"\n" +
                    "    }\n" +
                    "  }\n" +
                    "}", // <1>
                    XContentType.JSON);
                // end::put-mapping-request-source
                AcknowledgedResponse putMappingResponse = client.indices().putMapping(request, RequestOptions.DEFAULT);
                assertTrue(putMappingResponse.isAcknowledged());
            }

            {
                //tag::put-mapping-map
                Map<String, Object> jsonMap = new HashMap<>();
                Map<String, Object> message = new HashMap<>();
                message.put("type", "text");
                Map<String, Object> properties = new HashMap<>();
                properties.put("message", message);
                jsonMap.put("properties", properties);
                request.source(jsonMap); // <1>
                //end::put-mapping-map
                AcknowledgedResponse putMappingResponse = client.indices().putMapping(request, RequestOptions.DEFAULT);
                assertTrue(putMappingResponse.isAcknowledged());
            }
            {
                //tag::put-mapping-xcontent
                XContentBuilder builder = XContentFactory.jsonBuilder();
                builder.startObject();
                {
                    builder.startObject("properties");
                    {
                        builder.startObject("message");
                        {
                            builder.field("type", "text");
                        }
                        builder.endObject();
                    }
                    builder.endObject();
                }
                builder.endObject();
                request.source(builder); // <1>
                //end::put-mapping-xcontent
                AcknowledgedResponse putMappingResponse = client.indices().putMapping(request, RequestOptions.DEFAULT);
                assertTrue(putMappingResponse.isAcknowledged());
            }

            // tag::put-mapping-request-timeout
            request.setTimeout(TimeValue.timeValueMinutes(2)); // <1>
            // end::put-mapping-request-timeout

            // tag::put-mapping-request-masterTimeout
            request.setMasterTimeout(TimeValue.timeValueMinutes(1)); // <1>
            // end::put-mapping-request-masterTimeout

            // tag::put-mapping-execute
            AcknowledgedResponse putMappingResponse = client.indices().putMapping(request, RequestOptions.DEFAULT);
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
            CreateIndexResponse createIndexResponse = client.indices().create(new CreateIndexRequest("twitter"), RequestOptions.DEFAULT);
            assertTrue(createIndexResponse.isAcknowledged());
        }

        {
            PutMappingRequest request = new PutMappingRequest("twitter");

            // tag::put-mapping-execute-listener
            ActionListener<AcknowledgedResponse> listener =
                new ActionListener<AcknowledgedResponse>() {
                    @Override
                    public void onResponse(AcknowledgedResponse putMappingResponse) {
                        // <1>
                    }

                    @Override
                    public void onFailure(Exception e) {
                        // <2>
                    }
                };
            // end::put-mapping-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::put-mapping-execute-async
            client.indices().putMappingAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::put-mapping-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testGetMapping() throws IOException {
        RestHighLevelClient client = highLevelClient();

        {
            CreateIndexResponse createIndexResponse = client.indices().create(new CreateIndexRequest("twitter"), RequestOptions.DEFAULT);
            assertTrue(createIndexResponse.isAcknowledged());
            PutMappingRequest request = new PutMappingRequest("twitter");
            request.source("{ \"properties\": { \"message\": { \"type\": \"text\" } } }",
                XContentType.JSON
            );
            AcknowledgedResponse putMappingResponse = client.indices().putMapping(request, RequestOptions.DEFAULT);
            assertTrue(putMappingResponse.isAcknowledged());
        }

        {
            // tag::get-mappings-request
            GetMappingsRequest request = new GetMappingsRequest(); // <1>
            request.indices("twitter"); // <2>
            // end::get-mappings-request

            // tag::get-mappings-request-masterTimeout
            request.setMasterTimeout(TimeValue.timeValueMinutes(1)); // <1>
            // end::get-mappings-request-masterTimeout

            // tag::get-mappings-request-indicesOptions
            request.indicesOptions(IndicesOptions.lenientExpandOpen()); // <1>
            // end::get-mappings-request-indicesOptions

            // tag::get-mappings-execute
            GetMappingsResponse getMappingResponse = client.indices().getMapping(request, RequestOptions.DEFAULT);
            // end::get-mappings-execute

            // tag::get-mappings-response
            Map<String, MappingMetadata> allMappings = getMappingResponse.mappings(); // <1>
            MappingMetadata indexMapping = allMappings.get("twitter"); // <2>
            Map<String, Object> mapping = indexMapping.sourceAsMap(); // <3>
            // end::get-mappings-response

            Map<String, String> type = new HashMap<>();
            type.put("type", "text");
            Map<String, Object> field = new HashMap<>();
            field.put("message", type);
            Map<String, Object> expected = new HashMap<>();
            expected.put("properties", field);
            assertThat(mapping, equalTo(expected));
        }
    }

    public void testGetMappingAsync() throws Exception {
        final RestHighLevelClient client = highLevelClient();

        {
            CreateIndexResponse createIndexResponse = client.indices().create(new CreateIndexRequest("twitter"), RequestOptions.DEFAULT);
            assertTrue(createIndexResponse.isAcknowledged());
            PutMappingRequest request = new PutMappingRequest("twitter");
            request.source("{ \"properties\": { \"message\": { \"type\": \"text\" } } }",
                XContentType.JSON
            );
            AcknowledgedResponse putMappingResponse = client.indices().putMapping(request, RequestOptions.DEFAULT);
            assertTrue(putMappingResponse.isAcknowledged());
        }

        {
            GetMappingsRequest request = new GetMappingsRequest();
            request.indices("twitter");

            // tag::get-mappings-execute-listener
            ActionListener<GetMappingsResponse> listener =
                new ActionListener<GetMappingsResponse>() {
                    @Override
                    public void onResponse(GetMappingsResponse putMappingResponse) {
                        // <1>
                    }

                    @Override
                    public void onFailure(Exception e) {
                        // <2>
                    }
                };
            // end::get-mappings-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            final ActionListener<GetMappingsResponse> latchListener = new LatchedActionListener<>(listener, latch);
            listener = ActionListener.wrap(r -> {
                Map<String, MappingMetadata> allMappings = r.mappings();
                MappingMetadata indexMapping = allMappings.get("twitter");
                Map<String, Object> mapping = indexMapping.sourceAsMap();

                Map<String, String> type = new HashMap<>();
                type.put("type", "text");
                Map<String, Object> field = new HashMap<>();
                field.put("message", type);
                Map<String, Object> expected = new HashMap<>();
                expected.put("properties", field);
                assertThat(mapping, equalTo(expected));
                latchListener.onResponse(r);
            }, e -> {
                latchListener.onFailure(e);
                fail("should not fail");
            });

            // tag::get-mappings-execute-async
            client.indices().getMappingAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::get-mappings-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    @SuppressWarnings("unused")
    public void testGetFieldMapping() throws IOException, InterruptedException {
        RestHighLevelClient client = highLevelClient();

        {
            CreateIndexResponse createIndexResponse = client.indices().create(new CreateIndexRequest("twitter"), RequestOptions.DEFAULT);
            assertTrue(createIndexResponse.isAcknowledged());
            PutMappingRequest request = new PutMappingRequest("twitter");
            request.source(
                "{\n" +
                    "  \"properties\": {\n" +
                    "    \"message\": {\n" +
                    "      \"type\": \"text\"\n" +
                    "    },\n" +
                    "    \"timestamp\": {\n" +
                    "      \"type\": \"date\"\n" +
                    "    }\n" +
                    "  }\n" +
                    "}", // <1>
                XContentType.JSON);
            AcknowledgedResponse putMappingResponse = client.indices().putMapping(request, RequestOptions.DEFAULT);
            assertTrue(putMappingResponse.isAcknowledged());
        }

        // tag::get-field-mappings-request
        GetFieldMappingsRequest request = new GetFieldMappingsRequest(); // <1>
        request.indices("twitter"); // <2>
        request.fields("message", "timestamp"); // <3>
        // end::get-field-mappings-request

        // tag::get-field-mappings-request-indicesOptions
        request.indicesOptions(IndicesOptions.lenientExpandOpen()); // <1>
        // end::get-field-mappings-request-indicesOptions

        {
            // tag::get-field-mappings-execute
            GetFieldMappingsResponse response =
                client.indices().getFieldMapping(request, RequestOptions.DEFAULT);
            // end::get-field-mappings-execute

            // tag::get-field-mappings-response
            final Map<String, Map<String, GetFieldMappingsResponse.FieldMappingMetadata>> mappings =
                response.mappings();// <1>
            final Map<String, GetFieldMappingsResponse.FieldMappingMetadata> fieldMappings =
                mappings.get("twitter"); // <2>
            final GetFieldMappingsResponse.FieldMappingMetadata metadata =
                fieldMappings.get("message");// <3>

            final String fullName = metadata.fullName();// <4>
            final Map<String, Object> source = metadata.sourceAsMap(); // <5>
            // end::get-field-mappings-response
        }

        {
            // tag::get-field-mappings-execute-listener
            ActionListener<GetFieldMappingsResponse> listener =
                new ActionListener<GetFieldMappingsResponse>() {
                    @Override
                    public void onResponse(GetFieldMappingsResponse putMappingResponse) {
                        // <1>
                    }

                    @Override
                    public void onFailure(Exception e) {
                        // <2>
                    }
                };
            // end::get-field-mappings-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            final ActionListener<GetFieldMappingsResponse> latchListener = new LatchedActionListener<>(listener, latch);
            listener = ActionListener.wrap(r -> {
                final Map<String, Map<String, GetFieldMappingsResponse.FieldMappingMetadata>> mappings =
                    r.mappings();
                final Map<String, GetFieldMappingsResponse.FieldMappingMetadata> fieldMappings =
                    mappings.get("twitter");
                final GetFieldMappingsResponse.FieldMappingMetadata metadata1 = fieldMappings.get("message");

                final String fullName = metadata1.fullName();
                final Map<String, Object> source = metadata1.sourceAsMap();
                latchListener.onResponse(r);
            }, e -> {
                latchListener.onFailure(e);
                fail("should not fail");
            });

            // tag::get-field-mappings-execute-async
            client.indices().getFieldMappingAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::get-field-mappings-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }


    }


    public void testOpenIndex() throws Exception {
        RestHighLevelClient client = highLevelClient();

        {
            CreateIndexResponse createIndexResponse = client.indices().create(new CreateIndexRequest("index"), RequestOptions.DEFAULT);
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
            OpenIndexResponse openIndexResponse = client.indices().open(request, RequestOptions.DEFAULT);
            // end::open-index-execute

            // tag::open-index-response
            boolean acknowledged = openIndexResponse.isAcknowledged(); // <1>
            boolean shardsAcked = openIndexResponse.isShardsAcknowledged(); // <2>
            // end::open-index-response
            assertTrue(acknowledged);
            assertTrue(shardsAcked);

            // tag::open-index-execute-listener
            ActionListener<OpenIndexResponse> listener =
                    new ActionListener<OpenIndexResponse>() {
                @Override
                public void onResponse(OpenIndexResponse openIndexResponse) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            // end::open-index-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::open-index-execute-async
            client.indices().openAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::open-index-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }

        {
            // tag::open-index-notfound
            try {
                OpenIndexRequest request = new OpenIndexRequest("does_not_exist");
                client.indices().open(request, RequestOptions.DEFAULT);
            } catch (ElasticsearchException exception) {
                if (exception.status() == RestStatus.BAD_REQUEST) {
                    // <1>
                }
            }
            // end::open-index-notfound
        }
    }

    @SuppressWarnings("unused")
    public void testRefreshIndex() throws Exception {
        RestHighLevelClient client = highLevelClient();

        {
            createIndex("index1", Settings.EMPTY);
        }

        {
            // tag::refresh-request
            RefreshRequest request = new RefreshRequest("index1"); // <1>
            RefreshRequest requestMultiple = new RefreshRequest("index1", "index2"); // <2>
            RefreshRequest requestAll = new RefreshRequest(); // <3>
            // end::refresh-request

            // tag::refresh-request-indicesOptions
            request.indicesOptions(IndicesOptions.lenientExpandOpen()); // <1>
            // end::refresh-request-indicesOptions

            // tag::refresh-execute
            RefreshResponse refreshResponse = client.indices().refresh(request, RequestOptions.DEFAULT);
            // end::refresh-execute

            // tag::refresh-response
            int totalShards = refreshResponse.getTotalShards(); // <1>
            int successfulShards = refreshResponse.getSuccessfulShards(); // <2>
            int failedShards = refreshResponse.getFailedShards(); // <3>
            DefaultShardOperationFailedException[] failures = refreshResponse.getShardFailures(); // <4>
            // end::refresh-response

            // tag::refresh-execute-listener
            ActionListener<RefreshResponse> listener = new ActionListener<RefreshResponse>() {
                @Override
                public void onResponse(RefreshResponse refreshResponse) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            // end::refresh-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::refresh-execute-async
            client.indices().refreshAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::refresh-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }

        {
            // tag::refresh-notfound
            try {
                RefreshRequest request = new RefreshRequest("does_not_exist");
                client.indices().refresh(request, RequestOptions.DEFAULT);
            } catch (ElasticsearchException exception) {
                if (exception.status() == RestStatus.NOT_FOUND) {
                    // <1>
                }
            }
            // end::refresh-notfound
        }
    }

    @SuppressWarnings("unused")
    public void testFlushIndex() throws Exception {
        RestHighLevelClient client = highLevelClient();

        {
            createIndex("index1", Settings.EMPTY);
        }

        {
            // tag::flush-request
            FlushRequest request = new FlushRequest("index1"); // <1>
            FlushRequest requestMultiple = new FlushRequest("index1", "index2"); // <2>
            FlushRequest requestAll = new FlushRequest(); // <3>
            // end::flush-request

            // tag::flush-request-indicesOptions
            request.indicesOptions(IndicesOptions.lenientExpandOpen()); // <1>
            // end::flush-request-indicesOptions

            // tag::flush-request-wait
            request.waitIfOngoing(true); // <1>
            // end::flush-request-wait

            // tag::flush-request-force
            request.force(true); // <1>
            // end::flush-request-force

            // tag::flush-execute
            FlushResponse flushResponse = client.indices().flush(request, RequestOptions.DEFAULT);
            // end::flush-execute

            // tag::flush-response
            int totalShards = flushResponse.getTotalShards(); // <1>
            int successfulShards = flushResponse.getSuccessfulShards(); // <2>
            int failedShards = flushResponse.getFailedShards(); // <3>
            DefaultShardOperationFailedException[] failures = flushResponse.getShardFailures(); // <4>
            // end::flush-response

            // tag::flush-execute-listener
            ActionListener<FlushResponse> listener = new ActionListener<FlushResponse>() {
                @Override
                public void onResponse(FlushResponse flushResponse) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            // end::flush-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::flush-execute-async
            client.indices().flushAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::flush-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }

        {
            // tag::flush-notfound
            try {
                FlushRequest request = new FlushRequest("does_not_exist");
                client.indices().flush(request, RequestOptions.DEFAULT);
            } catch (ElasticsearchException exception) {
                if (exception.status() == RestStatus.NOT_FOUND) {
                    // <1>
                }
            }
            // end::flush-notfound
        }
    }

    public void testGetSettings() throws Exception {
        RestHighLevelClient client = highLevelClient();

        {
            Settings settings = Settings.builder().put("number_of_shards", 3).build();
            CreateIndexResponse createIndexResponse = client.indices().create(
                new CreateIndexRequest("index").settings(settings), RequestOptions.DEFAULT);
            assertTrue(createIndexResponse.isAcknowledged());
        }

        // tag::get-settings-request
        GetSettingsRequest request = new GetSettingsRequest().indices("index"); // <1>
        // end::get-settings-request

        // tag::get-settings-request-names
        request.names("index.number_of_shards"); // <1>
        // end::get-settings-request-names

        // tag::get-settings-request-indicesOptions
        request.indicesOptions(IndicesOptions.lenientExpandOpen()); // <1>
        // end::get-settings-request-indicesOptions

        // tag::get-settings-execute
        GetSettingsResponse getSettingsResponse = client.indices().getSettings(request, RequestOptions.DEFAULT);
        // end::get-settings-execute

        // tag::get-settings-response
        String numberOfShardsString = getSettingsResponse.getSetting("index", "index.number_of_shards"); // <1>
        Settings indexSettings = getSettingsResponse.getIndexToSettings().get("index"); // <2>
        Integer numberOfShards = indexSettings.getAsInt("index.number_of_shards", null); // <3>
        // end::get-settings-response

        assertEquals("3", numberOfShardsString);
        assertEquals(Integer.valueOf(3), numberOfShards);

        assertNull("refresh_interval returned but was never set!",
            getSettingsResponse.getSetting("index", "index.refresh_interval"));

        // tag::get-settings-execute-listener
        ActionListener<GetSettingsResponse> listener =
            new ActionListener<GetSettingsResponse>() {
                @Override
                public void onResponse(GetSettingsResponse GetSettingsResponse) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
        // end::get-settings-execute-listener

        // Replace the empty listener by a blocking listener in test
        final CountDownLatch latch = new CountDownLatch(1);
        listener = new LatchedActionListener<>(listener, latch);

        // tag::get-settings-execute-async
        client.indices().getSettingsAsync(request, RequestOptions.DEFAULT, listener); // <1>
        // end::get-settings-execute-async

        assertTrue(latch.await(30L, TimeUnit.SECONDS));
    }

    public void testGetSettingsWithDefaults() throws Exception {
        RestHighLevelClient client = highLevelClient();

        {
            Settings settings = Settings.builder().put("number_of_shards", 3).build();
            CreateIndexResponse createIndexResponse = client.indices().create(
                new CreateIndexRequest("index").settings(settings), RequestOptions.DEFAULT);
            assertTrue(createIndexResponse.isAcknowledged());
        }

        GetSettingsRequest request = new GetSettingsRequest().indices("index");
        request.indicesOptions(IndicesOptions.lenientExpandOpen());

        // tag::get-settings-request-include-defaults
        request.includeDefaults(true); // <1>
        // end::get-settings-request-include-defaults

        GetSettingsResponse getSettingsResponse = client.indices().getSettings(request, RequestOptions.DEFAULT);
        String numberOfShardsString = getSettingsResponse.getSetting("index", "index.number_of_shards");
        Settings indexSettings = getSettingsResponse.getIndexToSettings().get("index");
        Integer numberOfShards = indexSettings.getAsInt("index.number_of_shards", null);

        // tag::get-settings-defaults-response
        String refreshInterval = getSettingsResponse.getSetting("index", "index.refresh_interval"); // <1>
        Settings indexDefaultSettings = getSettingsResponse.getIndexToDefaultSettings().get("index"); // <2>
        // end::get-settings-defaults-response

        assertEquals("3", numberOfShardsString);
        assertEquals(Integer.valueOf(3), numberOfShards);
        assertNotNull("with defaults enabled we should get a value for refresh_interval!", refreshInterval);

        assertEquals(refreshInterval, indexDefaultSettings.get("index.refresh_interval"));
        ActionListener<GetSettingsResponse> listener =
            new ActionListener<GetSettingsResponse>() {
                @Override
                public void onResponse(GetSettingsResponse GetSettingsResponse) {
                }

                @Override
                public void onFailure(Exception e) {
                }
            };

        // Replace the empty listener by a blocking listener in test
        final CountDownLatch latch = new CountDownLatch(1);
        listener = new LatchedActionListener<>(listener, latch);

        client.indices().getSettingsAsync(request, RequestOptions.DEFAULT, listener);
        assertTrue(latch.await(30L, TimeUnit.SECONDS));
    }

    public void testGetIndex() throws Exception {
        RestHighLevelClient client = highLevelClient();

        {
            Settings settings = Settings.builder().put("number_of_shards", 3).build();
            String mappings = "{\"properties\":{\"field-1\":{\"type\":\"integer\"}}}";
            CreateIndexRequest createIndexRequest = new CreateIndexRequest("index")
                .settings(settings)
                .mapping(mappings, XContentType.JSON);
            CreateIndexResponse createIndexResponse = client.indices().create(
                createIndexRequest, RequestOptions.DEFAULT);
            assertTrue(createIndexResponse.isAcknowledged());
        }

        // tag::get-index-request
        GetIndexRequest request = new GetIndexRequest("index"); // <1>
        // end::get-index-request

        // tag::get-index-request-indicesOptions
        request.indicesOptions(IndicesOptions.lenientExpandOpen()); // <1>
        // end::get-index-request-indicesOptions

        // tag::get-index-request-includeDefaults
        request.includeDefaults(true); // <1>
        // end::get-index-request-includeDefaults

        // tag::get-index-execute
        GetIndexResponse getIndexResponse = client.indices().get(request, RequestOptions.DEFAULT);
        // end::get-index-execute

        // tag::get-index-response
        MappingMetadata indexMappings = getIndexResponse.getMappings().get("index"); // <1>
        Map<String, Object> indexTypeMappings = indexMappings.getSourceAsMap(); // <2>
        List<AliasMetadata> indexAliases = getIndexResponse.getAliases().get("index"); // <3>
        String numberOfShardsString = getIndexResponse.getSetting("index", "index.number_of_shards"); // <4>
        Settings indexSettings = getIndexResponse.getSettings().get("index"); // <5>
        Integer numberOfShards = indexSettings.getAsInt("index.number_of_shards", null); // <6>
        TimeValue time = getIndexResponse.getDefaultSettings().get("index")
            .getAsTime("index.refresh_interval", null); // <7>
        // end::get-index-response

        assertEquals(
            Collections.singletonMap("properties",
                Collections.singletonMap("field-1", Collections.singletonMap("type", "integer"))),
            indexTypeMappings
        );
        assertTrue(indexAliases.isEmpty());
        assertEquals(IndexSettings.DEFAULT_REFRESH_INTERVAL, time);
        assertEquals("3", numberOfShardsString);
        assertEquals(Integer.valueOf(3), numberOfShards);

        // tag::get-index-execute-listener
        ActionListener<GetIndexResponse> listener =
            new ActionListener<GetIndexResponse>() {
                @Override
                public void onResponse(GetIndexResponse getIndexResponse) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
        // end::get-index-execute-listener

        // Replace the empty listener by a blocking listener in test
        final CountDownLatch latch = new CountDownLatch(1);
        listener = new LatchedActionListener<>(listener, latch);

        // tag::get-index-execute-async
        client.indices().getAsync(request, RequestOptions.DEFAULT, listener); // <1>
        // end::get-index-execute-async

        assertTrue(latch.await(30L, TimeUnit.SECONDS));
    }

    @SuppressWarnings("unused")
    public void testForceMergeIndex() throws Exception {
        RestHighLevelClient client = highLevelClient();

        {
            createIndex("index", Settings.EMPTY);
        }

        {
            // tag::force-merge-request
            ForceMergeRequest request = new ForceMergeRequest("index1"); // <1>
            ForceMergeRequest requestMultiple = new ForceMergeRequest("index1", "index2"); // <2>
            ForceMergeRequest requestAll = new ForceMergeRequest(); // <3>
            // end::force-merge-request

            // tag::force-merge-request-indicesOptions
            request.indicesOptions(IndicesOptions.lenientExpandOpen()); // <1>
            // end::force-merge-request-indicesOptions

            // tag::force-merge-request-segments-num
            request.maxNumSegments(1); // <1>
            // end::force-merge-request-segments-num

            // tag::force-merge-request-only-expunge-deletes
            request.onlyExpungeDeletes(true); // <1>
            // end::force-merge-request-only-expunge-deletes

            // set only expunge deletes back to its default value
            // as it is mutually exclusive with max. num. segments
            request.onlyExpungeDeletes(ForceMergeRequest.Defaults.ONLY_EXPUNGE_DELETES);

            // tag::force-merge-request-flush
            request.flush(true); // <1>
            // end::force-merge-request-flush

            // tag::force-merge-execute
            ForceMergeResponse forceMergeResponse = client.indices().forcemerge(request, RequestOptions.DEFAULT);
            // end::force-merge-execute

            // tag::force-merge-response
            int totalShards = forceMergeResponse.getTotalShards(); // <1>
            int successfulShards = forceMergeResponse.getSuccessfulShards(); // <2>
            int failedShards = forceMergeResponse.getFailedShards(); // <3>
            DefaultShardOperationFailedException[] failures = forceMergeResponse.getShardFailures(); // <4>
            // end::force-merge-response

            // tag::force-merge-execute-listener
            ActionListener<ForceMergeResponse> listener = new ActionListener<ForceMergeResponse>() {
                @Override
                public void onResponse(ForceMergeResponse forceMergeResponse) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            // end::force-merge-execute-listener

            // tag::force-merge-execute-async
            client.indices().forcemergeAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::force-merge-execute-async
        }
        {
            // tag::force-merge-notfound
            try {
                ForceMergeRequest request = new ForceMergeRequest("does_not_exist");
                client.indices().forcemerge(request, RequestOptions.DEFAULT);
            } catch (ElasticsearchException exception) {
                if (exception.status() == RestStatus.NOT_FOUND) {
                    // <1>
                }
            }
            // end::force-merge-notfound
        }
    }

    @SuppressWarnings("unused")
    public void testClearCache() throws Exception {
        RestHighLevelClient client = highLevelClient();

        {
            createIndex("index1", Settings.EMPTY);
        }

        {
            // tag::clear-cache-request
            ClearIndicesCacheRequest request = new ClearIndicesCacheRequest("index1"); // <1>
            ClearIndicesCacheRequest requestMultiple = new ClearIndicesCacheRequest("index1", "index2"); // <2>
            ClearIndicesCacheRequest requestAll = new ClearIndicesCacheRequest(); // <3>
            // end::clear-cache-request

            // tag::clear-cache-request-indicesOptions
            request.indicesOptions(IndicesOptions.lenientExpandOpen()); // <1>
            // end::clear-cache-request-indicesOptions

            // tag::clear-cache-request-query
            request.queryCache(true); // <1>
            // end::clear-cache-request-query

            // tag::clear-cache-request-request
            request.requestCache(true); // <1>
            // end::clear-cache-request-request

            // tag::clear-cache-request-fielddata
            request.fieldDataCache(true); // <1>
            // end::clear-cache-request-fielddata

            // tag::clear-cache-request-fields
            request.fields("field1", "field2", "field3"); // <1>
            // end::clear-cache-request-fields

            // tag::clear-cache-execute
            ClearIndicesCacheResponse clearCacheResponse = client.indices().clearCache(request, RequestOptions.DEFAULT);
            // end::clear-cache-execute

            // tag::clear-cache-response
            int totalShards = clearCacheResponse.getTotalShards(); // <1>
            int successfulShards = clearCacheResponse.getSuccessfulShards(); // <2>
            int failedShards = clearCacheResponse.getFailedShards(); // <3>
            DefaultShardOperationFailedException[] failures = clearCacheResponse.getShardFailures(); // <4>
            // end::clear-cache-response

            // tag::clear-cache-execute-listener
            ActionListener<ClearIndicesCacheResponse> listener = new ActionListener<ClearIndicesCacheResponse>() {
                @Override
                public void onResponse(ClearIndicesCacheResponse clearCacheResponse) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            // end::clear-cache-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::clear-cache-execute-async
            client.indices().clearCacheAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::clear-cache-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }

        {
            // tag::clear-cache-notfound
            try {
                ClearIndicesCacheRequest request = new ClearIndicesCacheRequest("does_not_exist");
                client.indices().clearCache(request, RequestOptions.DEFAULT);
            } catch (ElasticsearchException exception) {
                if (exception.status() == RestStatus.NOT_FOUND) {
                    // <1>
                }
            }
            // end::clear-cache-notfound
        }
    }

    public void testCloseIndex() throws Exception {
        RestHighLevelClient client = highLevelClient();

        {
            CreateIndexResponse createIndexResponse = client.indices().create(new CreateIndexRequest("index"), RequestOptions.DEFAULT);
            assertTrue(createIndexResponse.isAcknowledged());
        }

        {
            // tag::close-index-request
            CloseIndexRequest request = new CloseIndexRequest("index"); // <1>
            // end::close-index-request

            // tag::close-index-request-timeout
            request.setTimeout(TimeValue.timeValueMinutes(2)); // <1>
            // end::close-index-request-timeout
            // tag::close-index-request-masterTimeout
            request.setMasterTimeout(TimeValue.timeValueMinutes(1)); // <1>
            // end::close-index-request-masterTimeout

            // tag::close-index-request-indicesOptions
            request.indicesOptions(IndicesOptions.lenientExpandOpen()); // <1>
            // end::close-index-request-indicesOptions

            // tag::close-index-execute
            AcknowledgedResponse closeIndexResponse = client.indices().close(request, RequestOptions.DEFAULT);
            // end::close-index-execute

            // tag::close-index-response
            boolean acknowledged = closeIndexResponse.isAcknowledged(); // <1>
            // end::close-index-response
            assertTrue(acknowledged);

            // tag::close-index-execute-listener
            ActionListener<CloseIndexResponse> listener = new ActionListener<>() {
                @Override
                public void onResponse(CloseIndexResponse closeIndexResponse) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            // end::close-index-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::close-index-execute-async
            client.indices().closeAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::close-index-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    @SuppressWarnings("unused")
    public void testExistsAlias() throws Exception {
        RestHighLevelClient client = highLevelClient();

        {
            CreateIndexResponse createIndexResponse = client.indices().create(new CreateIndexRequest("index")
                    .alias(new Alias("alias")), RequestOptions.DEFAULT);
            assertTrue(createIndexResponse.isAcknowledged());
        }

        {
            // tag::exists-alias-request
            GetAliasesRequest request = new GetAliasesRequest();
            GetAliasesRequest requestWithAlias = new GetAliasesRequest("alias1");
            GetAliasesRequest requestWithAliases =
                    new GetAliasesRequest(new String[]{"alias1", "alias2"});
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
            boolean exists = client.indices().existsAlias(request, RequestOptions.DEFAULT);
            // end::exists-alias-execute
            assertTrue(exists);

            // tag::exists-alias-execute-listener
            ActionListener<Boolean> listener = new ActionListener<Boolean>() {
                @Override
                public void onResponse(Boolean exists) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            // end::exists-alias-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::exists-alias-execute-async
            client.indices().existsAliasAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::exists-alias-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    @SuppressWarnings("unused")
    public void testUpdateAliases() throws Exception {
        RestHighLevelClient client = highLevelClient();

        {
            CreateIndexResponse createIndexResponse = client.indices().create(new CreateIndexRequest("index1"), RequestOptions.DEFAULT);
            assertTrue(createIndexResponse.isAcknowledged());
            createIndexResponse = client.indices().create(new CreateIndexRequest("index2"), RequestOptions.DEFAULT);
            assertTrue(createIndexResponse.isAcknowledged());
            createIndexResponse = client.indices().create(new CreateIndexRequest("index3"), RequestOptions.DEFAULT);
            assertTrue(createIndexResponse.isAcknowledged());
            createIndexResponse = client.indices().create(new CreateIndexRequest("index4"), RequestOptions.DEFAULT);
            assertTrue(createIndexResponse.isAcknowledged());
        }

        {
            // tag::update-aliases-request
            IndicesAliasesRequest request = new IndicesAliasesRequest(); // <1>
            AliasActions aliasAction =
                    new AliasActions(AliasActions.Type.ADD)
                    .index("index1")
                    .alias("alias1"); // <2>
            request.addAliasAction(aliasAction); // <3>
            // end::update-aliases-request

            // tag::update-aliases-request2
            AliasActions addIndexAction =
                    new AliasActions(AliasActions.Type.ADD)
                    .index("index1")
                    .alias("alias1")
                    .filter("{\"term\":{\"year\":2016}}"); // <1>
            AliasActions addIndicesAction =
                    new AliasActions(AliasActions.Type.ADD)
                    .indices("index1", "index2")
                    .alias("alias2")
                    .routing("1"); // <2>
            AliasActions removeAction =
                    new AliasActions(AliasActions.Type.REMOVE)
                    .index("index3")
                    .alias("alias3"); // <3>
            AliasActions removeIndexAction =
                    new AliasActions(AliasActions.Type.REMOVE_INDEX)
                    .index("index4"); // <4>
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
            AcknowledgedResponse indicesAliasesResponse =
                    client.indices().updateAliases(request, RequestOptions.DEFAULT);
            // end::update-aliases-execute

            // tag::update-aliases-response
            boolean acknowledged = indicesAliasesResponse.isAcknowledged(); // <1>
            // end::update-aliases-response
            assertTrue(acknowledged);
        }

        {
            IndicesAliasesRequest request = new IndicesAliasesRequest();
            AliasActions aliasAction = new AliasActions(AliasActions.Type.ADD).index("index1").alias("async");
            request.addAliasAction(aliasAction);

            // tag::update-aliases-execute-listener
            ActionListener<AcknowledgedResponse> listener =
                    new ActionListener<AcknowledgedResponse>() {
                @Override
                public void onResponse(AcknowledgedResponse indicesAliasesResponse) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            // end::update-aliases-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::update-aliases-execute-async
            client.indices().updateAliasesAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::update-aliases-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testShrinkIndex() throws Exception {
        RestHighLevelClient client = highLevelClient();

        {
            Map<String, Object> nodes = getAsMap("_nodes");
            @SuppressWarnings("unchecked")
            String firstNode = ((Map<String, Object>) nodes.get("nodes")).keySet().iterator().next();
            createIndex("source_index", Settings.builder().put("index.number_of_shards", 4).put("index.number_of_replicas", 0).build());
            updateIndexSettings("source_index", Settings.builder().put("index.routing.allocation.require._name", firstNode)
                    .put("index.blocks.write", true));
        }

        // tag::shrink-index-request
        ResizeRequest request = new ResizeRequest("target_index","source_index"); // <1>
        // end::shrink-index-request

        // tag::shrink-index-request-timeout
        request.timeout(TimeValue.timeValueMinutes(2)); // <1>
        request.timeout("2m"); // <2>
        // end::shrink-index-request-timeout
        // tag::shrink-index-request-masterTimeout
        request.masterNodeTimeout(TimeValue.timeValueMinutes(1)); // <1>
        request.masterNodeTimeout("1m"); // <2>
        // end::shrink-index-request-masterTimeout
        // tag::shrink-index-request-waitForActiveShards
        request.setWaitForActiveShards(2); // <1>
        request.setWaitForActiveShards(ActiveShardCount.DEFAULT); // <2>
        // end::shrink-index-request-waitForActiveShards
        // tag::shrink-index-request-settings
        request.getTargetIndexRequest().settings(Settings.builder()
                .put("index.number_of_shards", 2) // <1>
                .putNull("index.routing.allocation.require._name")); // <2>
        // end::shrink-index-request-settings
        // tag::shrink-index-request-aliases
        request.getTargetIndexRequest().alias(new Alias("target_alias")); // <1>
        // end::shrink-index-request-aliases

        // tag::shrink-index-execute
        ResizeResponse resizeResponse = client.indices().shrink(request, RequestOptions.DEFAULT);
        // end::shrink-index-execute

        // tag::shrink-index-response
        boolean acknowledged = resizeResponse.isAcknowledged(); // <1>
        boolean shardsAcked = resizeResponse.isShardsAcknowledged(); // <2>
        // end::shrink-index-response
        assertTrue(acknowledged);
        assertTrue(shardsAcked);

        // tag::shrink-index-execute-listener
        ActionListener<ResizeResponse> listener = new ActionListener<ResizeResponse>() {
            @Override
            public void onResponse(ResizeResponse resizeResponse) {
                // <1>
            }

            @Override
            public void onFailure(Exception e) {
                // <2>
            }
        };
        // end::shrink-index-execute-listener

        // Replace the empty listener by a blocking listener in test
        final CountDownLatch latch = new CountDownLatch(1);
        listener = new LatchedActionListener<>(listener, latch);

        // tag::shrink-index-execute-async
        client.indices().shrinkAsync(request, RequestOptions.DEFAULT, listener); // <1>
        // end::shrink-index-execute-async

        assertTrue(latch.await(30L, TimeUnit.SECONDS));
    }

    public void testSplitIndex() throws Exception {
        RestHighLevelClient client = highLevelClient();

        {
            createIndex("source_index", Settings.builder().put("index.number_of_shards", 2).put("index.number_of_replicas", 0)
                    .put("index.number_of_routing_shards", 4).build());
            updateIndexSettings("source_index", Settings.builder().put("index.blocks.write", true));
        }

        // tag::split-index-request
        ResizeRequest request = new ResizeRequest("target_index","source_index"); // <1>
        request.setResizeType(ResizeType.SPLIT); // <2>
        // end::split-index-request

        // tag::split-index-request-timeout
        request.timeout(TimeValue.timeValueMinutes(2)); // <1>
        request.timeout("2m"); // <2>
        // end::split-index-request-timeout
        // tag::split-index-request-masterTimeout
        request.masterNodeTimeout(TimeValue.timeValueMinutes(1)); // <1>
        request.masterNodeTimeout("1m"); // <2>
        // end::split-index-request-masterTimeout
        // tag::split-index-request-waitForActiveShards
        request.setWaitForActiveShards(2); // <1>
        request.setWaitForActiveShards(ActiveShardCount.DEFAULT); // <2>
        // end::split-index-request-waitForActiveShards
        // tag::split-index-request-settings
        request.getTargetIndexRequest().settings(Settings.builder()
                .put("index.number_of_shards", 4)); // <1>
        // end::split-index-request-settings
        // tag::split-index-request-aliases
        request.getTargetIndexRequest().alias(new Alias("target_alias")); // <1>
        // end::split-index-request-aliases

        // tag::split-index-execute
        ResizeResponse resizeResponse = client.indices().split(request, RequestOptions.DEFAULT);
        // end::split-index-execute

        // tag::split-index-response
        boolean acknowledged = resizeResponse.isAcknowledged(); // <1>
        boolean shardsAcked = resizeResponse.isShardsAcknowledged(); // <2>
        // end::split-index-response
        assertTrue(acknowledged);
        assertTrue(shardsAcked);

        // tag::split-index-execute-listener
        ActionListener<ResizeResponse> listener = new ActionListener<ResizeResponse>() {
            @Override
            public void onResponse(ResizeResponse resizeResponse) {
                // <1>
            }

            @Override
            public void onFailure(Exception e) {
                // <2>
            }
        };
        // end::split-index-execute-listener

        // Replace the empty listener by a blocking listener in test
        final CountDownLatch latch = new CountDownLatch(1);
        listener = new LatchedActionListener<>(listener, latch);

        // tag::split-index-execute-async
        client.indices().splitAsync(request, RequestOptions.DEFAULT,listener); // <1>
        // end::split-index-execute-async

        assertTrue(latch.await(30L, TimeUnit.SECONDS));
    }

    public void testCloneIndex() throws Exception {
        RestHighLevelClient client = highLevelClient();

        {
            createIndex("source_index", Settings.builder().put("index.number_of_shards", 2).put("index.number_of_replicas", 0).build());
            updateIndexSettings("source_index", Settings.builder().put("index.blocks.write", true));
        }

        // tag::clone-index-request
        ResizeRequest request = new ResizeRequest("target_index","source_index"); // <1>
        request.setResizeType(ResizeType.CLONE); // <2>
        // end::clone-index-request

        // tag::clone-index-request-timeout
        request.timeout(TimeValue.timeValueMinutes(2)); // <1>
        request.timeout("2m"); // <2>
        // end::clone-index-request-timeout
        // tag::clone-index-request-masterTimeout
        request.masterNodeTimeout(TimeValue.timeValueMinutes(1)); // <1>
        request.masterNodeTimeout("1m"); // <2>
        // end::clone-index-request-masterTimeout
        // tag::clone-index-request-waitForActiveShards
        request.setWaitForActiveShards(2); // <1>
        request.setWaitForActiveShards(ActiveShardCount.DEFAULT); // <2>
        // end::clone-index-request-waitForActiveShards
        // tag::clone-index-request-settings
        request.getTargetIndexRequest().settings(Settings.builder()
            .put("index.number_of_shards", 2)); // <1>
        // end::clone-index-request-settings
        // tag::clone-index-request-aliases
        request.getTargetIndexRequest().alias(new Alias("target_alias")); // <1>
        // end::clone-index-request-aliases

        // tag::clone-index-execute
        ResizeResponse resizeResponse = client.indices().clone(request, RequestOptions.DEFAULT);
        // end::clone-index-execute

        // tag::clone-index-response
        boolean acknowledged = resizeResponse.isAcknowledged(); // <1>
        boolean shardsAcked = resizeResponse.isShardsAcknowledged(); // <2>
        // end::clone-index-response
        assertTrue(acknowledged);
        assertTrue(shardsAcked);

        // tag::clone-index-execute-listener
        ActionListener<ResizeResponse> listener = new ActionListener<ResizeResponse>() {
            @Override
            public void onResponse(ResizeResponse resizeResponse) {
                // <1>
            }

            @Override
            public void onFailure(Exception e) {
                // <2>
            }
        };
        // end::clone-index-execute-listener

        // Replace the empty listener by a blocking listener in test
        final CountDownLatch latch = new CountDownLatch(1);
        listener = new LatchedActionListener<>(listener, latch);

        // tag::clone-index-execute-async
        client.indices().cloneAsync(request, RequestOptions.DEFAULT,listener); // <1>
        // end::clone-index-execute-async

        assertTrue(latch.await(30L, TimeUnit.SECONDS));
    }

    public void testRolloverIndex() throws Exception {
        RestHighLevelClient client = highLevelClient();

        {
            client.indices().create(new CreateIndexRequest("index-1").alias(new Alias("alias")), RequestOptions.DEFAULT);
        }

        // tag::rollover-index-request
        RolloverRequest request = new RolloverRequest("alias", "index-2"); // <1>
        request.addMaxIndexAgeCondition(new TimeValue(7, TimeUnit.DAYS)); // <2>
        request.addMaxIndexDocsCondition(1000); // <3>
        request.addMaxIndexSizeCondition(new ByteSizeValue(5, ByteSizeUnit.GB)); // <4>
        // end::rollover-index-request

        // tag::rollover-index-request-timeout
        request.setTimeout(TimeValue.timeValueMinutes(2)); // <1>
        // end::rollover-index-request-timeout
        // tag::rollover-index-request-masterTimeout
        request.setMasterTimeout(TimeValue.timeValueMinutes(1)); // <1>
        // end::rollover-index-request-masterTimeout
        // tag::rollover-index-request-dryRun
        request.dryRun(true); // <1>
        // end::rollover-index-request-dryRun
        // tag::rollover-index-request-waitForActiveShards
        request.getCreateIndexRequest().waitForActiveShards(ActiveShardCount.from(2)); // <1>
        request.getCreateIndexRequest().waitForActiveShards(ActiveShardCount.DEFAULT); // <2>
        // end::rollover-index-request-waitForActiveShards
        // tag::rollover-index-request-settings
        request.getCreateIndexRequest().settings(Settings.builder()
                .put("index.number_of_shards", 4)); // <1>
        // end::rollover-index-request-settings
        // tag::rollover-index-request-mapping
        String mappings = "{\"properties\":{\"field-1\":{\"type\":\"keyword\"}}}";
        request.getCreateIndexRequest().mapping(mappings, XContentType.JSON); // <1>
        // end::rollover-index-request-mapping
        // tag::rollover-index-request-alias
        request.getCreateIndexRequest().alias(new Alias("another_alias")); // <1>
        // end::rollover-index-request-alias

        // tag::rollover-index-execute
        RolloverResponse rolloverResponse = client.indices().rollover(request, RequestOptions.DEFAULT);
        // end::rollover-index-execute

        // tag::rollover-index-response
        boolean acknowledged = rolloverResponse.isAcknowledged(); // <1>
        boolean shardsAcked = rolloverResponse.isShardsAcknowledged(); // <2>
        String oldIndex = rolloverResponse.getOldIndex(); // <3>
        String newIndex = rolloverResponse.getNewIndex(); // <4>
        boolean isRolledOver = rolloverResponse.isRolledOver(); // <5>
        boolean isDryRun = rolloverResponse.isDryRun(); // <6>
        Map<String, Boolean> conditionStatus = rolloverResponse.getConditionStatus();// <7>
        // end::rollover-index-response
        assertFalse(acknowledged);
        assertFalse(shardsAcked);
        assertEquals("index-1", oldIndex);
        assertEquals("index-2", newIndex);
        assertFalse(isRolledOver);
        assertTrue(isDryRun);
        assertEquals(3, conditionStatus.size());

        // tag::rollover-index-execute-listener
        ActionListener<RolloverResponse> listener = new ActionListener<RolloverResponse>() {
            @Override
            public void onResponse(RolloverResponse rolloverResponse) {
                // <1>
            }

            @Override
            public void onFailure(Exception e) {
                // <2>
            }
        };
        // end::rollover-index-execute-listener

        // Replace the empty listener by a blocking listener in test
        final CountDownLatch latch = new CountDownLatch(1);
        listener = new LatchedActionListener<>(listener, latch);

        // tag::rollover-index-execute-async
        client.indices().rolloverAsync(request, RequestOptions.DEFAULT, listener); // <1>
        // end::rollover-index-execute-async

        assertTrue(latch.await(30L, TimeUnit.SECONDS));
    }

    @SuppressWarnings("unused")
    public void testGetAlias() throws Exception {
        RestHighLevelClient client = highLevelClient();

        {
            CreateIndexResponse createIndexResponse = client.indices().create(new CreateIndexRequest("index").alias(new Alias("alias")),
                    RequestOptions.DEFAULT);
            assertTrue(createIndexResponse.isAcknowledged());
        }

        {
            // tag::get-alias-request
            GetAliasesRequest request = new GetAliasesRequest();
            GetAliasesRequest requestWithAlias = new GetAliasesRequest("alias1");
            GetAliasesRequest requestWithAliases =
                    new GetAliasesRequest(new String[]{"alias1", "alias2"});
            // end::get-alias-request

            // tag::get-alias-request-alias
            request.aliases("alias"); // <1>
            // end::get-alias-request-alias
            // tag::get-alias-request-indices
            request.indices("index"); // <1>
            // end::get-alias-request-indices

            // tag::get-alias-request-indicesOptions
            request.indicesOptions(IndicesOptions.lenientExpandOpen()); // <1>
            // end::get-alias-request-indicesOptions

            // tag::get-alias-request-local
            request.local(true); // <1>
            // end::get-alias-request-local

            // tag::get-alias-execute
            GetAliasesResponse response = client.indices().getAlias(request, RequestOptions.DEFAULT);
            // end::get-alias-execute

            // tag::get-alias-response
            Map<String, Set<AliasMetadata>> aliases = response.getAliases(); // <1>
            // end::get-alias-response

            // tag::get-alias-response-error
            RestStatus status = response.status(); // <1>
            ElasticsearchException exception = response.getException(); // <2>
            String error = response.getError(); // <3>
            // end::get-alias-response-error

            assertThat(response.getAliases().get("index").size(), equalTo(1));
            assertThat(response.getAliases().get("index").iterator().next().alias(), equalTo("alias"));
            assertThat(status, equalTo(RestStatus.OK));
            assertThat(error, nullValue());
            assertThat(exception, nullValue());

            // tag::get-alias-execute-listener
            ActionListener<GetAliasesResponse> listener =
                    new ActionListener<GetAliasesResponse>() {
                        @Override
                        public void onResponse(GetAliasesResponse getAliasesResponse) {
                            // <1>
                        }

                        @Override
                        public void onFailure(Exception e) {
                            // <2>
                        }
            };
            // end::get-alias-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::get-alias-execute-async
            client.indices().getAliasAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::get-alias-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    @SuppressWarnings("unused")
    public void testIndexPutSettings() throws Exception {
        RestHighLevelClient client = highLevelClient();

        {
            CreateIndexResponse createIndexResponse = client.indices().create(new CreateIndexRequest("index"), RequestOptions.DEFAULT);
            assertTrue(createIndexResponse.isAcknowledged());
        }

        // tag::indices-put-settings-request
        UpdateSettingsRequest request = new UpdateSettingsRequest("index1"); // <1>
        UpdateSettingsRequest requestMultiple =
                new UpdateSettingsRequest("index1", "index2"); // <2>
        UpdateSettingsRequest requestAll = new UpdateSettingsRequest(); // <3>
        // end::indices-put-settings-request

        // tag::indices-put-settings-create-settings
        String settingKey = "index.number_of_replicas";
        int settingValue = 0;
        Settings settings =
                Settings.builder()
                .put(settingKey, settingValue)
                .build(); // <1>
        // end::indices-put-settings-create-settings
        // tag::indices-put-settings-request-index-settings
        request.settings(settings);
        // end::indices-put-settings-request-index-settings

        {
            // tag::indices-put-settings-settings-builder
            Settings.Builder settingsBuilder =
                    Settings.builder()
                    .put(settingKey, settingValue);
            request.settings(settingsBuilder); // <1>
            // end::indices-put-settings-settings-builder
        }
        {
            // tag::indices-put-settings-settings-map
            Map<String, Object> map = new HashMap<>();
            map.put(settingKey, settingValue);
            request.settings(map); // <1>
            // end::indices-put-settings-settings-map
        }
        {
            // tag::indices-put-settings-settings-source
            request.settings(
                    "{\"index.number_of_replicas\": \"2\"}"
                    , XContentType.JSON); // <1>
            // end::indices-put-settings-settings-source
        }

        // tag::indices-put-settings-request-preserveExisting
        request.setPreserveExisting(false); // <1>
        // end::indices-put-settings-request-preserveExisting
        // tag::indices-put-settings-request-timeout
        request.timeout(TimeValue.timeValueMinutes(2)); // <1>
        request.timeout("2m"); // <2>
        // end::indices-put-settings-request-timeout
        // tag::indices-put-settings-request-masterTimeout
        request.masterNodeTimeout(TimeValue.timeValueMinutes(1)); // <1>
        request.masterNodeTimeout("1m"); // <2>
        // end::indices-put-settings-request-masterTimeout
        // tag::indices-put-settings-request-indicesOptions
        request.indicesOptions(IndicesOptions.lenientExpandOpen()); // <1>
        // end::indices-put-settings-request-indicesOptions

        // tag::indices-put-settings-execute
        AcknowledgedResponse updateSettingsResponse =
                client.indices().putSettings(request, RequestOptions.DEFAULT);
        // end::indices-put-settings-execute

        // tag::indices-put-settings-response
        boolean acknowledged = updateSettingsResponse.isAcknowledged(); // <1>
        // end::indices-put-settings-response
        assertTrue(acknowledged);

        // tag::indices-put-settings-execute-listener
        ActionListener<AcknowledgedResponse> listener =
                new ActionListener<AcknowledgedResponse>() {

            @Override
            public void onResponse(AcknowledgedResponse updateSettingsResponse) {
                // <1>
            }

            @Override
            public void onFailure(Exception e) {
                // <2>
            }
        };
        // end::indices-put-settings-execute-listener

        // Replace the empty listener by a blocking listener in test
        final CountDownLatch latch = new CountDownLatch(1);
        listener = new LatchedActionListener<>(listener, latch);

        // tag::indices-put-settings-execute-async
        client.indices().putSettingsAsync(request, RequestOptions.DEFAULT, listener); // <1>
        // end::indices-put-settings-execute-async

        assertTrue(latch.await(30L, TimeUnit.SECONDS));
    }

    public void testPutTemplate() throws Exception {
        RestHighLevelClient client = highLevelClient();

        // tag::put-template-request
        PutIndexTemplateRequest request = new PutIndexTemplateRequest(
            "my-template", // <1>
            List.of("pattern-1", "log-*") // <2>
        );
        // end::put-template-request

        // tag::put-template-request-settings
        request.settings(Settings.builder() // <1>
            .put("index.number_of_shards", 3)
            .put("index.number_of_replicas", 1)
        );
        // end::put-template-request-settings

        {
            // tag::put-template-request-mappings-json
            request.mapping(// <1>
                "{\n" +
                    "  \"properties\": {\n" +
                    "    \"message\": {\n" +
                    "      \"type\": \"text\"\n" +
                    "    }\n" +
                    "  }\n" +
                    "}",
                XContentType.JSON);
            // end::put-template-request-mappings-json
            assertTrue(client.indices().putTemplate(request, RequestOptions.DEFAULT).isAcknowledged());
        }
        {
            //tag::put-template-request-mappings-map
            Map<String, Object> jsonMap = new HashMap<>();
            {
                Map<String, Object> properties = new HashMap<>();
                {
                    Map<String, Object> message = new HashMap<>();
                    message.put("type", "text");
                    properties.put("message", message);
                }
                jsonMap.put("properties", properties);
            }
            request.mapping(jsonMap); // <1>
            //end::put-template-request-mappings-map
            assertTrue(client.indices().putTemplate(request, RequestOptions.DEFAULT).isAcknowledged());
        }
        {
            //tag::put-template-request-mappings-xcontent
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            {
                builder.startObject("properties");
                {
                    builder.startObject("message");
                    {
                        builder.field("type", "text");
                    }
                    builder.endObject();
                }
                builder.endObject();
            }
            builder.endObject();
            request.mapping(builder); // <1>
            //end::put-template-request-mappings-xcontent
            assertTrue(client.indices().putTemplate(request, RequestOptions.DEFAULT).isAcknowledged());
        }

        // tag::put-template-request-aliases
        request.alias(new Alias("twitter_alias").filter(QueryBuilders.termQuery("user", "kimchy")));  // <1>
        request.alias(new Alias("{index}_alias").searchRouting("xyz"));  // <2>
        // end::put-template-request-aliases

        // tag::put-template-request-order
        request.order(20);  // <1>
        // end::put-template-request-order

        // tag::put-template-request-version
        request.version(4);  // <1>
        // end::put-template-request-version

        // tag::put-template-whole-source
        request.source("{\n" +
            "  \"index_patterns\": [\n" +
            "    \"log-*\",\n" +
            "    \"pattern-1\"\n" +
            "  ],\n" +
            "  \"order\": 1,\n" +
            "  \"settings\": {\n" +
            "    \"number_of_shards\": 1\n" +
            "  },\n" +
            "  \"mappings\": {\n" +
            "    \"properties\": {\n" +
            "      \"message\": {\n" +
            "        \"type\": \"text\"\n" +
            "      }\n" +
            "    }\n" +
            "  },\n" +
            "  \"aliases\": {\n" +
            "    \"alias-1\": {},\n" +
            "    \"{index}-alias\": {}\n" +
            "  }\n" +
            "}", XContentType.JSON); // <1>
        // end::put-template-whole-source

        // tag::put-template-request-create
        request.create(true);  // <1>
        // end::put-template-request-create

        // tag::put-template-request-masterTimeout
        request.masterNodeTimeout(TimeValue.timeValueMinutes(1)); // <1>
        request.masterNodeTimeout("1m"); // <2>
        // end::put-template-request-masterTimeout

        request.create(false); // make test happy

        // tag::put-template-execute
        AcknowledgedResponse putTemplateResponse = client.indices().putTemplate(request, RequestOptions.DEFAULT);
        // end::put-template-execute

        // tag::put-template-response
        boolean acknowledged = putTemplateResponse.isAcknowledged(); // <1>
        // end::put-template-response
        assertTrue(acknowledged);

        // tag::put-template-execute-listener
        ActionListener<AcknowledgedResponse> listener =
            new ActionListener<AcknowledgedResponse>() {
                @Override
                public void onResponse(AcknowledgedResponse putTemplateResponse) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
        // end::put-template-execute-listener

        // Replace the empty listener by a blocking listener in test
        final CountDownLatch latch = new CountDownLatch(1);
        listener = new LatchedActionListener<>(listener, latch);

        // tag::put-template-execute-async
        client.indices().putTemplateAsync(request, RequestOptions.DEFAULT, listener); // <1>
        // end::put-template-execute-async

        assertTrue(latch.await(30L, TimeUnit.SECONDS));
    }

    public void testGetTemplates() throws Exception {
        RestHighLevelClient client = highLevelClient();
        {
            PutIndexTemplateRequest putRequest =
                new PutIndexTemplateRequest("my-template", List.of("pattern-1", "log-*"));
            putRequest.settings(Settings.builder().put("index.number_of_shards", 3).put("index.number_of_replicas", 1));
            putRequest.mapping("{ \"properties\": { \"message\": { \"type\": \"text\" } } }",
                XContentType.JSON
            );
            assertTrue(client.indices().putTemplate(putRequest, RequestOptions.DEFAULT).isAcknowledged());
        }

        // tag::get-templates-request
        GetIndexTemplatesRequest request = new GetIndexTemplatesRequest("my-template"); // <1>
        request = new GetIndexTemplatesRequest("template-1", "template-2"); // <2>
        request = new GetIndexTemplatesRequest("my-*"); // <3>
        // end::get-templates-request

        // tag::get-templates-request-masterTimeout
        request.setMasterNodeTimeout(TimeValue.timeValueMinutes(1)); // <1>
        request.setMasterNodeTimeout("1m"); // <2>
        // end::get-templates-request-masterTimeout

        // tag::get-templates-execute
        GetIndexTemplatesResponse getTemplatesResponse = client.indices().getIndexTemplate(request, RequestOptions.DEFAULT);
        // end::get-templates-execute

        // tag::get-templates-response
        List<IndexTemplateMetadata> templates = getTemplatesResponse.getIndexTemplates(); // <1>
        // end::get-templates-response

        assertThat(templates, hasSize(1));
        assertThat(templates.get(0).name(), equalTo("my-template"));

        // tag::get-templates-execute-listener
        ActionListener<GetIndexTemplatesResponse> listener =
            new ActionListener<GetIndexTemplatesResponse>() {
                @Override
                public void onResponse(GetIndexTemplatesResponse response) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
        // end::get-templates-execute-listener

        // Replace the empty listener by a blocking listener in test
        final CountDownLatch latch = new CountDownLatch(1);
        listener = new LatchedActionListener<>(listener, latch);

        // tag::get-templates-execute-async
        client.indices().getIndexTemplateAsync(request, RequestOptions.DEFAULT, listener); // <1>
        // end::get-templates-execute-async

        assertTrue(latch.await(30L, TimeUnit.SECONDS));
    }

    public void testGetIndexTemplatesV2() throws Exception {
        RestHighLevelClient client = highLevelClient();
        {
            Template template = new Template(Settings.builder().put("index.number_of_shards", 3).put("index.number_of_replicas", 1).build(),
                new CompressedXContent("{ \"properties\": { \"message\": { \"type\": \"text\" } } }"),
                null);
            PutIndexTemplateV2Request putRequest = new PutIndexTemplateV2Request()
                .name("my-template")
                .indexTemplate(
                    new IndexTemplateV2(List.of("pattern-1", "log-*"), template, null, null, null, null)
                );
            assertTrue(client.indices().putIndexTemplate(putRequest, RequestOptions.DEFAULT).isAcknowledged());
        }

        // tag::get-index-templates-v2-request
        GetIndexTemplateV2Request request = new GetIndexTemplateV2Request("my-template"); // <1>
        request = new GetIndexTemplateV2Request("my-*"); // <2>
        // end::get-index-templates-v2-request

        // tag::get-index-templates-v2-request-masterTimeout
        request.setMasterNodeTimeout(TimeValue.timeValueMinutes(1)); // <1>
        request.setMasterNodeTimeout("1m"); // <2>
        // end::get-index-templates-v2-request-masterTimeout

        // tag::get-index-templates-v2-execute
        GetIndexTemplatesV2Response getTemplatesResponse = client.indices().getIndexTemplate(request, RequestOptions.DEFAULT);
        // end::get-index-templates-v2-execute

        // tag::get-index-templates-v2-response
        Map<String, IndexTemplateV2> templates = getTemplatesResponse.getIndexTemplates(); // <1>
        // end::get-index-templates-v2-response

        assertThat(templates.size(), is(1));
        assertThat(templates.get("my-template"), is(notNullValue()));

        // tag::get-index-templates-v2-execute-listener
        ActionListener<GetIndexTemplatesV2Response> listener =
            new ActionListener<GetIndexTemplatesV2Response>() {
                @Override
                public void onResponse(GetIndexTemplatesV2Response response) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
        // end::get-index-templates-v2-execute-listener

        // Replace the empty listener by a blocking listener in test
        final CountDownLatch latch = new CountDownLatch(1);
        listener = new LatchedActionListener<>(listener, latch);

        // tag::get-index-templates-v2-execute-async
        client.indices().getIndexTemplateAsync(request, RequestOptions.DEFAULT, listener); // <1>
        // end::get-index-templates-v2-execute-async

        assertTrue(latch.await(30L, TimeUnit.SECONDS));
    }

    public void testPutIndexTemplateV2() throws Exception {
        RestHighLevelClient client = highLevelClient();

        {
            // tag::put-index-template-v2-request
            PutIndexTemplateV2Request request = new PutIndexTemplateV2Request()
                .name("my-template"); // <1>
            IndexTemplateV2 indexTemplateV2 = new IndexTemplateV2(List.of("pattern-1", "log-*"), null, null, null, null, null); // <2>
            request.indexTemplate(indexTemplateV2);
            assertTrue(client.indices().putIndexTemplate(request, RequestOptions.DEFAULT).isAcknowledged());
            // end::put-index-template-v2-request
        }

        {
            // tag::put-index-template-v2-request-settings
            PutIndexTemplateV2Request request = new PutIndexTemplateV2Request()
                .name("my-template");
            Settings settings = Settings.builder() // <1>
                .put("index.number_of_shards", 3)
                .put("index.number_of_replicas", 1)
                .build();
            Template template = new Template(settings, null, null); // <2>
            IndexTemplateV2 indexTemplateV2 = new IndexTemplateV2(List.of("pattern-1", "log-*"), template, null, null, null, null); // <3>
            request.indexTemplate(indexTemplateV2);

            assertTrue(client.indices().putIndexTemplate(request, RequestOptions.DEFAULT).isAcknowledged());
            // end::put-index-template-v2-request-settings
        }

        {
            // tag::put-index-template-v2-request-mappings-json
            String mappingJson = "{\n" +
                "  \"properties\": {\n" +
                "    \"message\": {\n" +
                "      \"type\": \"text\"\n" +
                "    }\n" +
                "  }\n" +
                "}"; // <1>
            PutIndexTemplateV2Request request = new PutIndexTemplateV2Request()
                .name("my-template");
            Template template = new Template(null, new CompressedXContent(mappingJson), null); // <2>
            IndexTemplateV2 indexTemplateV2 = new IndexTemplateV2(List.of("pattern-1", "log-*"), template, null, null, null, null); // <3>
            request.indexTemplate(indexTemplateV2);

            assertTrue(client.indices().putIndexTemplate(request, RequestOptions.DEFAULT).isAcknowledged());
            // end::put-index-template-v2-request-mappings-json
        }

        {
            // tag::put-index-template-v2-request-aliases
            PutIndexTemplateV2Request request = new PutIndexTemplateV2Request()
                .name("my-template");
            AliasMetadata twitterAlias = AliasMetadata.builder("twitter_alias").build(); // <1>
            AliasMetadata placeholderAlias = AliasMetadata.builder("{index}_alias").searchRouting("xyz").build(); // <2>
            Template template = new Template(null, null, Map.of("twitter_alias", twitterAlias, "{index}_alias", placeholderAlias)); // <3>
            IndexTemplateV2 indexTemplateV2 = new IndexTemplateV2(List.of("pattern-1", "log-*"), template, null, null, null, null); // <3>
            request.indexTemplate(indexTemplateV2);

            assertTrue(client.indices().putIndexTemplate(request, RequestOptions.DEFAULT).isAcknowledged());
            // end::put-index-template-v2-request-aliases
        }

        {
            Template template = new Template(Settings.builder().put("index.number_of_replicas", 3).build(), null, null);
            ComponentTemplate componentTemplate = new ComponentTemplate(template, null, null);
            client.cluster().putComponentTemplate(new PutComponentTemplateRequest().name("ct1").componentTemplate(componentTemplate),
                RequestOptions.DEFAULT);

            // tag::put-index-template-v2-request-component-template
            PutIndexTemplateV2Request request = new PutIndexTemplateV2Request()
                .name("my-template");
            IndexTemplateV2 indexTemplateV2 =
                new IndexTemplateV2(List.of("pattern-1", "log-*"), null, List.of("ct1"), null, null, null); // <1>
            request.indexTemplate(indexTemplateV2);

            assertTrue(client.indices().putIndexTemplate(request, RequestOptions.DEFAULT).isAcknowledged());
            // end::put-index-template-v2-request-component-template
        }

        {
            // tag::put-index-template-v2-request-priority
            PutIndexTemplateV2Request request = new PutIndexTemplateV2Request()
                .name("my-template");
            IndexTemplateV2 indexTemplateV2 = new IndexTemplateV2(List.of("pattern-1", "log-*"), null, null, 20L, null, null); // <1>
            request.indexTemplate(indexTemplateV2);

            assertTrue(client.indices().putIndexTemplate(request, RequestOptions.DEFAULT).isAcknowledged());
            // end::put-index-template-v2-request-priority
        }

        {
            // tag::put-index-template-v2-request-version
            PutIndexTemplateV2Request request = new PutIndexTemplateV2Request()
                .name("my-template");
            IndexTemplateV2 indexTemplateV2 = new IndexTemplateV2(List.of("pattern-1", "log-*"), null, null, null, 3L, null); // <1>
            request.indexTemplate(indexTemplateV2);

            assertTrue(client.indices().putIndexTemplate(request, RequestOptions.DEFAULT).isAcknowledged());
            // end::put-index-template-v2-request-version

            // tag::put-index-template-v2-request-create
            request.create(true);  // <1>
            // end::put-index-template-v2-request-create

            // tag::put-index-template-v2-request-masterTimeout
            request.setMasterTimeout(TimeValue.timeValueMinutes(1)); // <1>
            // end::put-index-template-v2-request-masterTimeout

            request.create(false); // make test happy

            // tag::put-index-template-v2-execute
            AcknowledgedResponse putTemplateResponse = client.indices().putIndexTemplate(request, RequestOptions.DEFAULT);
            // end::put-index-template-v2-execute

            // tag::put-index-template-v2-response
            boolean acknowledged = putTemplateResponse.isAcknowledged(); // <1>
            // end::put-index-template-v2-response
            assertTrue(acknowledged);

            // tag::put-index-template-v2-execute-listener
            ActionListener<AcknowledgedResponse> listener =
                new ActionListener<AcknowledgedResponse>() {
                    @Override
                    public void onResponse(AcknowledgedResponse putIndexTemplateResponse) {
                        // <1>
                    }

                    @Override
                    public void onFailure(Exception e) {
                        // <2>
                    }
                };
            // end::put-index-template-v2-execute-listener

            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::put-index-template-v2-execute-async
            client.indices().putIndexTemplateAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::put-index-template-v2-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testDeleteIndexTemplateV2() throws Exception {
        RestHighLevelClient client = highLevelClient();
        {
            PutIndexTemplateV2Request request = new PutIndexTemplateV2Request()
                .name("my-template");
            IndexTemplateV2 indexTemplateV2 = new IndexTemplateV2(List.of("pattern-1", "log-*"), null, null, null, null, null); // <2>
            request.indexTemplate(indexTemplateV2);
            assertTrue(client.indices().putIndexTemplate(request, RequestOptions.DEFAULT).isAcknowledged());
        }

        // tag::delete-index-template-v2-request
        DeleteIndexTemplateV2Request deleteRequest = new DeleteIndexTemplateV2Request("my-template"); // <1>
        // end::delete-index-template-v2-request

        // tag::delete-index-template-v2-request-masterTimeout
        deleteRequest.setMasterTimeout(TimeValue.timeValueMinutes(1)); // <1>
        // end::delete-index-template-v2-request-masterTimeout

        // tag::delete-index-template-v2-execute
        AcknowledgedResponse deleteTemplateAcknowledge = client.indices().deleteIndexTemplate(deleteRequest, RequestOptions.DEFAULT);
        // end::delete-index-template-v2-execute

        // tag::delete-index-template-v2-response
        boolean acknowledged = deleteTemplateAcknowledge.isAcknowledged(); // <1>
        // end::delete-index-template-v2-response
        assertThat(acknowledged, equalTo(true));

        {
            PutIndexTemplateV2Request request = new PutIndexTemplateV2Request()
                .name("my-template");
            IndexTemplateV2 indexTemplateV2 = new IndexTemplateV2(List.of("pattern-1", "log-*"), null, null, null, null, null); // <2>
            request.indexTemplate(indexTemplateV2);
            assertTrue(client.indices().putIndexTemplate(request, RequestOptions.DEFAULT).isAcknowledged());
        }

        // tag::delete-index-template-v2-execute-listener
        ActionListener<AcknowledgedResponse> listener =
            new ActionListener<AcknowledgedResponse>() {
                @Override
                public void onResponse(AcknowledgedResponse response) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
        // end::delete-index-template-v2-execute-listener

        final CountDownLatch latch = new CountDownLatch(1);
        listener = new LatchedActionListener<>(listener, latch);

        // tag::delete-index-template-v2-execute-async
        client.indices().deleteIndexTemplateAsync(deleteRequest, RequestOptions.DEFAULT, listener); // <1>
        // end::delete-index-template-v2-execute-async

        assertTrue(latch.await(30L, TimeUnit.SECONDS));
    }

    public void testSimulateIndexTemplate() throws Exception {
        RestHighLevelClient client = highLevelClient();

        {
            PutIndexTemplateV2Request request = new PutIndexTemplateV2Request()
                .name("my-template"); // <1>
            Template template = new Template(Settings.builder().put("index.number_of_replicas", 3).build(), null, null);
            IndexTemplateV2 indexTemplateV2 = new IndexTemplateV2(List.of("pattern-1", "log-*"), template, null, null, null, null);
            request.indexTemplate(indexTemplateV2);
            assertTrue(client.indices().putIndexTemplate(request, RequestOptions.DEFAULT).isAcknowledged());
        }

        // tag::simulate-index-template-request
        SimulateIndexTemplateRequest simulateRequest = new SimulateIndexTemplateRequest("log-000001"); // <1>
        PutIndexTemplateV2Request newIndexTemplateRequest = new PutIndexTemplateV2Request()
            .name("used-for-simulation");
        Settings settings = Settings.builder().put("index.number_of_shards", 6).build();
        Template template = new Template(settings, null, null); // <2>
        IndexTemplateV2 indexTemplateV2 = new IndexTemplateV2(List.of("log-*"), template, null, 90L, null, null);
        newIndexTemplateRequest.indexTemplate(indexTemplateV2);

        simulateRequest.indexTemplateV2Request(newIndexTemplateRequest); // <2>
        // end::simulate-index-template-request

        // tag::simulate-index-template-response
        SimulateIndexTemplateResponse simulateIndexTemplateResponse = client.indices().simulateIndexTemplate(simulateRequest,
            RequestOptions.DEFAULT);
        assertThat(simulateIndexTemplateResponse.resolvedTemplate().settings().get("index.number_of_shards"), is("6")); // <1>
        assertThat(simulateIndexTemplateResponse.overlappingTemplates().get("my-template"),
            containsInAnyOrder("pattern-1", "log-*")); // <2>
        // end::simulate-index-template-response

        // tag::simulate-index-template-execute-listener
        ActionListener<SimulateIndexTemplateResponse> listener =
            new ActionListener<SimulateIndexTemplateResponse>() {
                @Override
                public void onResponse(SimulateIndexTemplateResponse response) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
        // end::simulate-index-template-execute-listener

        final CountDownLatch latch = new CountDownLatch(1);
        listener = new LatchedActionListener<>(listener, latch);

        // tag::simulate-index-template-execute-async
        client.indices().simulateIndexTemplateAsync(simulateRequest, RequestOptions.DEFAULT, listener); // <1>
        // end::simulate-index-template-execute-async

        assertTrue(latch.await(30L, TimeUnit.SECONDS));
    }

    public void testTemplatesExist() throws Exception {
        final RestHighLevelClient client = highLevelClient();
        {
            final PutIndexTemplateRequest putRequest = new PutIndexTemplateRequest("my-template",
                List.of("foo"));
            assertTrue(client.indices().putTemplate(putRequest, RequestOptions.DEFAULT).isAcknowledged());
        }

        {
            // tag::templates-exist-request
            IndexTemplatesExistRequest request;
            request = new IndexTemplatesExistRequest("my-template"); // <1>
            request = new IndexTemplatesExistRequest("template-1", "template-2"); // <2>
            request = new IndexTemplatesExistRequest("my-*"); // <3>
            // end::templates-exist-request

            // tag::templates-exist-request-optionals
            request.setLocal(true); // <1>
            request.setMasterNodeTimeout(TimeValue.timeValueMinutes(1)); // <2>
            request.setMasterNodeTimeout("1m"); // <3>
            // end::templates-exist-request-optionals

            // tag::templates-exist-execute
            boolean exists = client.indices().existsTemplate(request, RequestOptions.DEFAULT);
            // end::templates-exist-execute
            assertTrue(exists);

            // tag::templates-exist-execute-listener
            ActionListener<Boolean> listener = new ActionListener<Boolean>() {
                @Override
                public void onResponse(Boolean aBoolean) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            // end::templates-exist-execute-listener

            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::templates-exist-execute-async
            client.indices().existsTemplateAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::templates-exist-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }

    }

    @SuppressWarnings("unused")
    public void testValidateQuery() throws IOException, InterruptedException {
        RestHighLevelClient client = highLevelClient();

        String index = "some_index";
        createIndex(index, Settings.EMPTY);

        // tag::indices-validate-query-request
        ValidateQueryRequest request = new ValidateQueryRequest(index); // <1>
        // end::indices-validate-query-request

        // tag::indices-validate-query-request-query
        QueryBuilder builder = QueryBuilders
            .boolQuery() // <1>
            .must(QueryBuilders.queryStringQuery("*:*"))
            .filter(QueryBuilders.termQuery("user", "kimchy"));
        request.query(builder); // <2>
        // end::indices-validate-query-request-query

        // tag::indices-validate-query-request-explain
        request.explain(true); // <1>
        // end::indices-validate-query-request-explain

        // tag::indices-validate-query-request-allShards
        request.allShards(true); // <1>
        // end::indices-validate-query-request-allShards

        // tag::indices-validate-query-request-rewrite
        request.rewrite(true); // <1>
        // end::indices-validate-query-request-rewrite

        // tag::indices-validate-query-execute
        ValidateQueryResponse response = client.indices().validateQuery(request, RequestOptions.DEFAULT); // <1>
        // end::indices-validate-query-execute

        // tag::indices-validate-query-response
        boolean isValid = response.isValid(); // <1>
        int totalShards = response.getTotalShards(); // <2>
        int successfulShards = response.getSuccessfulShards(); // <3>
        int failedShards = response.getFailedShards(); // <4>
        if (failedShards > 0) {
            for(DefaultShardOperationFailedException failure: response.getShardFailures()) { // <5>
                String failedIndex = failure.index(); // <6>
                int shardId = failure.shardId(); // <7>
                String reason = failure.reason(); // <8>
            }
        }
        for(QueryExplanation explanation: response.getQueryExplanation()) { // <9>
            String explanationIndex = explanation.getIndex(); // <10>
            int shardId = explanation.getShard(); // <11>
            String explanationString = explanation.getExplanation(); // <12>
        }
        // end::indices-validate-query-response

        // tag::indices-validate-query-execute-listener
        ActionListener<ValidateQueryResponse> listener =
            new ActionListener<ValidateQueryResponse>() {
                @Override
                public void onResponse(ValidateQueryResponse validateQueryResponse) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
        // end::indices-validate-query-execute-listener

        // Replace the empty listener by a blocking listener in test
        final CountDownLatch latch = new CountDownLatch(1);
        listener = new LatchedActionListener<>(listener, latch);

        // tag::indices-validate-query-execute-async
        client.indices().validateQueryAsync(request, RequestOptions.DEFAULT, listener); // <1>
        // end::indices-validate-query-execute-async

        assertTrue(latch.await(30L, TimeUnit.SECONDS));
    }

    public void testAnalyze() throws IOException, InterruptedException {

        RestHighLevelClient client = highLevelClient();

        {
            // tag::analyze-builtin-request
            AnalyzeRequest request = AnalyzeRequest.withGlobalAnalyzer("english", // <1>
                "Some text to analyze", "Some more text to analyze");       // <2>
            // end::analyze-builtin-request
        }

        {
            // tag::analyze-custom-request
            Map<String, Object> stopFilter = new HashMap<>();
            stopFilter.put("type", "stop");
            stopFilter.put("stopwords", new String[]{ "to" });  // <1>
            AnalyzeRequest request = AnalyzeRequest.buildCustomAnalyzer("standard")  // <2>
                .addCharFilter("html_strip")    // <3>
                .addTokenFilter("lowercase")    // <4>
                .addTokenFilter(stopFilter)     // <5>
                .build("<b>Some text to analyze</b>");
            // end::analyze-custom-request
        }

        {
            // tag::analyze-custom-normalizer-request
            AnalyzeRequest request = AnalyzeRequest.buildCustomNormalizer()
                .addTokenFilter("lowercase")
                .build("<b>BaR</b>");
            // end::analyze-custom-normalizer-request

            // tag::analyze-request-explain
            request.explain(true);                      // <1>
            request.attributes("keyword", "type");      // <2>
            // end::analyze-request-explain

            // tag::analyze-execute
            AnalyzeResponse response = client.indices().analyze(request, RequestOptions.DEFAULT);
            // end::analyze-execute

            // tag::analyze-response-tokens
            List<AnalyzeResponse.AnalyzeToken> tokens = response.getTokens();   // <1>
            // end::analyze-response-tokens
            // tag::analyze-response-detail
            DetailAnalyzeResponse detail = response.detail();                   // <1>
            // end::analyze-response-detail

            assertNull(tokens);
            assertNotNull(detail.tokenizer());
        }

        CreateIndexRequest req = new CreateIndexRequest("my_index");
        CreateIndexResponse resp = client.indices().create(req, RequestOptions.DEFAULT);
        assertTrue(resp.isAcknowledged());

        PutMappingRequest pmReq = new PutMappingRequest("my_index")
            .source(XContentFactory.jsonBuilder().startObject()
                .startObject("properties")
                    .startObject("my_field")
                        .field("type", "text")
                        .field("analyzer", "english")
                    .endObject()
                .endObject()
            .endObject());
        AcknowledgedResponse pmResp = client.indices().putMapping(pmReq, RequestOptions.DEFAULT);
        assertTrue(pmResp.isAcknowledged());

        {
            // tag::analyze-index-request
            AnalyzeRequest request = AnalyzeRequest.withIndexAnalyzer(
                "my_index",         // <1>
                "my_analyzer",        // <2>
                "some text to analyze"
            );
            // end::analyze-index-request

            // tag::analyze-execute-listener
            ActionListener<AnalyzeResponse> listener = new ActionListener<AnalyzeResponse>() {
                @Override
                public void onResponse(AnalyzeResponse analyzeTokens) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            // end::analyze-execute-listener

            // use a built-in analyzer in the test
            request = AnalyzeRequest.withField("my_index", "my_field", "some text to analyze");
            // Use a blocking listener in the test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::analyze-execute-async
            client.indices().analyzeAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::analyze-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }

        {
            // tag::analyze-index-normalizer-request
            AnalyzeRequest request = AnalyzeRequest.withNormalizer(
                "my_index",             // <1>
                "my_normalizer",        // <2>
                "some text to analyze"
            );
            // end::analyze-index-normalizer-request
        }

        {
            // tag::analyze-field-request
            AnalyzeRequest request = AnalyzeRequest.withField("my_index", "my_field", "some text to analyze");
            // end::analyze-field-request
        }

    }

    public void testFreezeIndex() throws Exception {
        RestHighLevelClient client = highLevelClient();

        {
            CreateIndexResponse createIndexResponse = client.indices().create(new CreateIndexRequest("index"), RequestOptions.DEFAULT);
            assertTrue(createIndexResponse.isAcknowledged());
        }

        {
            // tag::freeze-index-request
            FreezeIndexRequest request = new FreezeIndexRequest("index"); // <1>
            // end::freeze-index-request

            // tag::freeze-index-request-timeout
            request.setTimeout(TimeValue.timeValueMinutes(2)); // <1>
            // end::freeze-index-request-timeout
            // tag::freeze-index-request-masterTimeout
            request.setMasterTimeout(TimeValue.timeValueMinutes(1)); // <1>
            // end::freeze-index-request-masterTimeout
            // tag::freeze-index-request-waitForActiveShards
            request.setWaitForActiveShards(ActiveShardCount.DEFAULT); // <1>
            // end::freeze-index-request-waitForActiveShards

            // tag::freeze-index-request-indicesOptions
            request.setIndicesOptions(IndicesOptions.strictExpandOpen()); // <1>
            // end::freeze-index-request-indicesOptions

            // tag::freeze-index-execute
            ShardsAcknowledgedResponse openIndexResponse = client.indices().freeze(request, RequestOptions.DEFAULT);
            // end::freeze-index-execute

            // tag::freeze-index-response
            boolean acknowledged = openIndexResponse.isAcknowledged(); // <1>
            boolean shardsAcked = openIndexResponse.isShardsAcknowledged(); // <2>
            // end::freeze-index-response
            assertTrue(acknowledged);
            assertTrue(shardsAcked);

            // tag::freeze-index-execute-listener
            ActionListener<ShardsAcknowledgedResponse> listener =
                new ActionListener<ShardsAcknowledgedResponse>() {
                    @Override
                    public void onResponse(ShardsAcknowledgedResponse freezeIndexResponse) {
                        // <1>
                    }

                    @Override
                    public void onFailure(Exception e) {
                        // <2>
                    }
                };
            // end::freeze-index-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::freeze-index-execute-async
            client.indices().freezeAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::freeze-index-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }

        {
            // tag::freeze-index-notfound
            try {
                FreezeIndexRequest request = new FreezeIndexRequest("does_not_exist");
                client.indices().freeze(request, RequestOptions.DEFAULT);
            } catch (ElasticsearchException exception) {
                if (exception.status() == RestStatus.BAD_REQUEST) {
                    // <1>
                }
            }
            // end::freeze-index-notfound
        }
    }

    public void testUnfreezeIndex() throws Exception {
        RestHighLevelClient client = highLevelClient();

        {
            CreateIndexResponse createIndexResponse = client.indices().create(new CreateIndexRequest("index"), RequestOptions.DEFAULT);
            assertTrue(createIndexResponse.isAcknowledged());
        }

        {
            // tag::unfreeze-index-request
            UnfreezeIndexRequest request = new UnfreezeIndexRequest("index"); // <1>
            // end::unfreeze-index-request

            // tag::unfreeze-index-request-timeout
            request.setTimeout(TimeValue.timeValueMinutes(2)); // <1>
            // end::unfreeze-index-request-timeout
            // tag::unfreeze-index-request-masterTimeout
            request.setMasterTimeout(TimeValue.timeValueMinutes(1)); // <1>
            // end::unfreeze-index-request-masterTimeout
            // tag::unfreeze-index-request-waitForActiveShards
            request.setWaitForActiveShards(ActiveShardCount.DEFAULT); // <1>
            // end::unfreeze-index-request-waitForActiveShards

            // tag::unfreeze-index-request-indicesOptions
            request.setIndicesOptions(IndicesOptions.strictExpandOpen()); // <1>
            // end::unfreeze-index-request-indicesOptions

            // tag::unfreeze-index-execute
            ShardsAcknowledgedResponse openIndexResponse = client.indices().unfreeze(request, RequestOptions.DEFAULT);
            // end::unfreeze-index-execute

            // tag::unfreeze-index-response
            boolean acknowledged = openIndexResponse.isAcknowledged(); // <1>
            boolean shardsAcked = openIndexResponse.isShardsAcknowledged(); // <2>
            // end::unfreeze-index-response
            assertTrue(acknowledged);
            assertTrue(shardsAcked);

            // tag::unfreeze-index-execute-listener
            ActionListener<ShardsAcknowledgedResponse> listener =
                new ActionListener<ShardsAcknowledgedResponse>() {
                    @Override
                    public void onResponse(ShardsAcknowledgedResponse freezeIndexResponse) {
                        // <1>
                    }

                    @Override
                    public void onFailure(Exception e) {
                        // <2>
                    }
                };
            // end::unfreeze-index-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::unfreeze-index-execute-async
            client.indices().unfreezeAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::unfreeze-index-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }

        {
            // tag::unfreeze-index-notfound
            try {
                UnfreezeIndexRequest request = new UnfreezeIndexRequest("does_not_exist");
                client.indices().unfreeze(request, RequestOptions.DEFAULT);
            } catch (ElasticsearchException exception) {
                if (exception.status() == RestStatus.BAD_REQUEST) {
                    // <1>
                }
            }
            // end::unfreeze-index-notfound
        }
    }

    public void testDeleteTemplate() throws Exception {
        RestHighLevelClient client = highLevelClient();
        {
            PutIndexTemplateRequest putRequest = new PutIndexTemplateRequest("my-template",
                List.of("pattern-1", "log-*"));
            putRequest.settings(Settings.builder().put("index.number_of_shards", 3));
            assertTrue(client.indices().putTemplate(putRequest, RequestOptions.DEFAULT).isAcknowledged());
        }

        // tag::delete-template-request
        DeleteIndexTemplateRequest request = new DeleteIndexTemplateRequest();
        request.name("my-template"); // <1>
        // end::delete-template-request

        // tag::delete-template-request-masterTimeout
        request.masterNodeTimeout(TimeValue.timeValueMinutes(1)); // <1>
        request.masterNodeTimeout("1m"); // <2>
        // end::delete-template-request-masterTimeout

        // tag::delete-template-execute
        AcknowledgedResponse deleteTemplateAcknowledge = client.indices().deleteTemplate(request, RequestOptions.DEFAULT);
        // end::delete-template-execute

        // tag::delete-template-response
        boolean acknowledged = deleteTemplateAcknowledge.isAcknowledged(); // <1>
        // end::delete-template-response
        assertThat(acknowledged, equalTo(true));

        {
            PutIndexTemplateRequest putRequest = new PutIndexTemplateRequest("my-template",
                List.of("pattern-1", "log-*"));
            putRequest.settings(Settings.builder().put("index.number_of_shards", 3));
            assertTrue(client.indices().putTemplate(putRequest, RequestOptions.DEFAULT).isAcknowledged());
        }
        // tag::delete-template-execute-listener
        ActionListener<AcknowledgedResponse> listener =
            new ActionListener<AcknowledgedResponse>() {
                @Override
                public void onResponse(AcknowledgedResponse response) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
        // end::delete-template-execute-listener

        // Replace the empty listener by a blocking listener in test
        final CountDownLatch latch = new CountDownLatch(1);
        listener = new LatchedActionListener<>(listener, latch);

        // tag::delete-template-execute-async
        client.indices().deleteTemplateAsync(request, RequestOptions.DEFAULT, listener); // <1>
        // end::delete-template-execute-async

        assertTrue(latch.await(30L, TimeUnit.SECONDS));
    }

    public void testReloadSearchAnalyzers() throws Exception {
        RestHighLevelClient client = highLevelClient();
        {
            CreateIndexResponse createIndexResponse = client.indices().create(new CreateIndexRequest("index"), RequestOptions.DEFAULT);
            assertTrue(createIndexResponse.isAcknowledged());
        }

        {
            // tag::reload-analyzers-request
            ReloadAnalyzersRequest request = new ReloadAnalyzersRequest("index"); // <1>
            // end::reload-analyzers-request

            // tag::reload-analyzers-request-indicesOptions
            request.setIndicesOptions(IndicesOptions.strictExpandOpen()); // <1>
            // end::reload-analyzers-request-indicesOptions

            // tag::reload-analyzers-execute
            ReloadAnalyzersResponse reloadResponse = client.indices().reloadAnalyzers(request, RequestOptions.DEFAULT);
            // end::reload-analyzers-execute

            // tag::reload-analyzers-response
            Shards shards = reloadResponse.shards(); // <1>
            Map<String, ReloadDetails> reloadDetails = reloadResponse.getReloadedDetails(); // <2>
            ReloadDetails details = reloadDetails.get("index"); // <3>
            String indexName = details.getIndexName(); // <4>
            Set<String> indicesNodes = details.getReloadedIndicesNodes(); // <5>
            Set<String> analyzers = details.getReloadedAnalyzers();  // <6>
            // end::reload-analyzers-response
            assertNotNull(shards);
            assertEquals("index", indexName);
            assertEquals(1, indicesNodes.size());
            assertEquals(0, analyzers.size());

            // tag::reload-analyzers-execute-listener
            ActionListener<ReloadAnalyzersResponse> listener =
                new ActionListener<ReloadAnalyzersResponse>() {
                    @Override
                    public void onResponse(ReloadAnalyzersResponse reloadResponse) {
                        // <1>
                    }

                    @Override
                    public void onFailure(Exception e) {
                        // <2>
                    }
                };
            // end::reload-analyzers-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::reload-analyzers-execute-async
            client.indices().reloadAnalyzersAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::reload-analyzers-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }

        {
            // tag::reload-analyzers-notfound
            try {
                ReloadAnalyzersRequest request = new ReloadAnalyzersRequest("does_not_exist");
                client.indices().reloadAnalyzers(request, RequestOptions.DEFAULT);
            } catch (ElasticsearchException exception) {
                if (exception.status() == RestStatus.BAD_REQUEST) {
                    // <1>
                }
            }
            // end::reload-analyzers-notfound
        }
    }

    @SuppressWarnings("unused")
    public void testDeleteAlias() throws Exception {
        RestHighLevelClient client = highLevelClient();

        {
            CreateIndexResponse createIndexResponse = client.indices().create(new CreateIndexRequest("index1"), RequestOptions.DEFAULT);
            assertTrue(createIndexResponse.isAcknowledged());
        }
        {
            IndicesAliasesRequest request = new IndicesAliasesRequest();
            AliasActions aliasAction =
                new AliasActions(AliasActions.Type.ADD)
                    .index("index1")
                    .alias("alias1");
            request.addAliasAction(aliasAction);
            AcknowledgedResponse indicesAliasesResponse =
                client.indices().updateAliases(request, RequestOptions.DEFAULT);
            assertTrue(indicesAliasesResponse.isAcknowledged());
        }
        {
            IndicesAliasesRequest request = new IndicesAliasesRequest();
            AliasActions aliasAction =
                new AliasActions(AliasActions.Type.ADD)
                    .index("index1")
                    .alias("alias2");
            request.addAliasAction(aliasAction);
            AcknowledgedResponse indicesAliasesResponse =
                client.indices().updateAliases(request, RequestOptions.DEFAULT);
            assertTrue(indicesAliasesResponse.isAcknowledged());
        }
        {
            // tag::delete-alias-request
            DeleteAliasRequest request = new DeleteAliasRequest("index1", "alias1");
            // end::delete-alias-request

            // tag::delete-alias-request-timeout
            request.setTimeout(TimeValue.timeValueMinutes(2)); // <1>
            // end::delete-alias-request-timeout
            // tag::delete-alias-request-masterTimeout
            request.setMasterTimeout(TimeValue.timeValueMinutes(1)); // <1>
            // end::delete-alias-request-masterTimeout

            // tag::delete-alias-execute
            org.elasticsearch.client.core.AcknowledgedResponse deleteAliasResponse =
                client.indices().deleteAlias(request, RequestOptions.DEFAULT);
            // end::delete-alias-execute

            // tag::delete-alias-response
            boolean acknowledged = deleteAliasResponse.isAcknowledged(); // <1>
            // end::delete-alias-response
            assertTrue(acknowledged);
        }

        {
            DeleteAliasRequest request = new DeleteAliasRequest("index1", "alias2"); // <1>

            // tag::delete-alias-execute-listener
            ActionListener<org.elasticsearch.client.core.AcknowledgedResponse> listener =
                new ActionListener<org.elasticsearch.client.core.AcknowledgedResponse>() {
                    @Override
                    public void onResponse(org.elasticsearch.client.core.AcknowledgedResponse deleteAliasResponse) {
                        // <1>
                    }

                    @Override
                    public void onFailure(Exception e) {
                        // <2>
                    }
                };
            // end::delete-alias-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::delete-alias-execute-async
            client.indices().deleteAliasAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::delete-alias-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }
}
