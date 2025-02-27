/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.template.put.PutComponentTemplateAction;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.ingest.SimulateIndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.metadata.ComponentTemplate;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class TransportSimulateBulkActionIT extends ESIntegTestCase {
    @SuppressWarnings("unchecked")
    public void testMappingValidationIndexExists() {
        /*
         * This test simulates a BulkRequest of two documents into an existing index. Then we make sure the index contains no documents, and
         * that the index's mapping in the cluster state has not been updated with the two new field.
         */
        String indexName = randomAlphaOfLength(20).toLowerCase(Locale.ROOT);
        String mapping = """
            {
              "_doc":{
                "dynamic":"strict",
                "properties":{
                  "foo1":{
                    "type":"text"
                  }
                }
              }
            }
            """;
        indicesAdmin().create(new CreateIndexRequest(indexName).mapping(mapping)).actionGet();
        BulkRequest bulkRequest = new SimulateBulkRequest(Map.of(), Map.of(), Map.of(), Map.of());
        bulkRequest.add(new IndexRequest(indexName).source("""
            {
              "foo1": "baz"
            }
            """, XContentType.JSON).id(randomUUID()));
        bulkRequest.add(new IndexRequest(indexName).source("""
            {
              "foo3": "baz"
            }
            """, XContentType.JSON).id(randomUUID()));
        BulkResponse response = client().execute(new ActionType<BulkResponse>(SimulateBulkAction.NAME), bulkRequest).actionGet();
        assertThat(response.getItems().length, equalTo(2));
        assertThat(response.getItems()[0].getResponse().getResult(), equalTo(DocWriteResponse.Result.CREATED));
        assertNull(((SimulateIndexResponse) response.getItems()[0].getResponse()).getException());
        assertThat(response.getItems()[1].getResponse().getResult(), equalTo(DocWriteResponse.Result.CREATED));
        assertThat(
            ((SimulateIndexResponse) response.getItems()[1].getResponse()).getException().getMessage(),
            containsString("mapping set to strict, dynamic introduction of")
        );
        indicesAdmin().refresh(new RefreshRequest(indexName)).actionGet();
        SearchResponse searchResponse = client().search(new SearchRequest(indexName)).actionGet();
        assertThat(searchResponse.getHits().getTotalHits().value(), equalTo(0L));
        searchResponse.decRef();
        ClusterStateResponse clusterStateResponse = admin().cluster().state(new ClusterStateRequest(TEST_REQUEST_TIMEOUT)).actionGet();
        Map<String, Object> indexMapping = clusterStateResponse.getState().metadata().getProject().index(indexName).mapping().sourceAsMap();
        Map<String, Object> fields = (Map<String, Object>) indexMapping.get("properties");
        assertThat(fields.size(), equalTo(1));
    }

    @SuppressWarnings("unchecked")
    public void testMappingValidationIndexExistsTemplateSubstitutions() throws IOException {
        /*
         * This test simulates a BulkRequest of two documents into an existing index. Then we make sure the index contains no documents, and
         * that the index's mapping in the cluster state has not been updated with the two new field. With the mapping from the template
         * that was used to create the index, we would expect the second document to throw an exception because it uses a field that does
         * not exist. But we substitute a new version of the component template named "test-component-template" that allows for the new
         * field.
         */
        String originalComponentTemplateMappingString = """
            {
              "_doc":{
                "dynamic":"strict",
                "properties":{
                  "foo1":{
                    "type":"text"
                  }
                }
              }
            }
            """;
        CompressedXContent mapping = CompressedXContent.fromJSON(originalComponentTemplateMappingString);
        Template template = new Template(Settings.EMPTY, mapping, null);
        PutComponentTemplateAction.Request componentTemplateActionRequest = new PutComponentTemplateAction.Request(
            "test-component-template"
        );
        ComponentTemplate componentTemplate = new ComponentTemplate(template, null, null);
        componentTemplateActionRequest.componentTemplate(componentTemplate);
        client().execute(PutComponentTemplateAction.INSTANCE, componentTemplateActionRequest).actionGet();
        ComposableIndexTemplate composableIndexTemplate = ComposableIndexTemplate.builder()
            .indexPatterns(List.of("my-index-*"))
            .componentTemplates(List.of("test-component-template"))
            .build();
        final String indexTemplateName = "test-index-template";
        TransportPutComposableIndexTemplateAction.Request request = new TransportPutComposableIndexTemplateAction.Request(
            indexTemplateName
        );
        request.indexTemplate(composableIndexTemplate);
        client().execute(TransportPutComposableIndexTemplateAction.TYPE, request).actionGet();

        String indexName = "my-index-1";
        // First, run before the index is created:
        assertMappingsUpdatedFromSubstitutions(indexName, indexTemplateName);
        // Now, create the index and make sure the component template substitutions work the same:
        indicesAdmin().create(new CreateIndexRequest(indexName)).actionGet();
        assertMappingsUpdatedFromSubstitutions(indexName, indexTemplateName);
        // Now make sure nothing was actually changed:
        indicesAdmin().refresh(new RefreshRequest(indexName)).actionGet();
        SearchResponse searchResponse = client().search(new SearchRequest(indexName)).actionGet();
        assertThat(searchResponse.getHits().getTotalHits().value(), equalTo(0L));
        searchResponse.decRef();
        ClusterStateResponse clusterStateResponse = admin().cluster().state(new ClusterStateRequest(TEST_REQUEST_TIMEOUT)).actionGet();
        Map<String, Object> indexMapping = clusterStateResponse.getState().metadata().getProject().index(indexName).mapping().sourceAsMap();
        Map<String, Object> fields = (Map<String, Object>) indexMapping.get("properties");
        assertThat(fields.size(), equalTo(1));
    }

    private void assertMappingsUpdatedFromSubstitutions(String indexName, String indexTemplateName) {
        IndexRequest indexRequest1 = new IndexRequest(indexName).source("""
            {
              "foo1": "baz"
            }
            """, XContentType.JSON).id(randomUUID());
        IndexRequest indexRequest2 = new IndexRequest(indexName).source("""
            {
              "foo3": "baz"
            }
            """, XContentType.JSON).id(randomUUID());
        {
            // First we use the original component template, and expect a failure in the second document:
            BulkRequest bulkRequest = new SimulateBulkRequest(Map.of(), Map.of(), Map.of(), Map.of());
            bulkRequest.add(indexRequest1);
            bulkRequest.add(indexRequest2);
            BulkResponse response = client().execute(new ActionType<BulkResponse>(SimulateBulkAction.NAME), bulkRequest).actionGet();
            assertThat(response.getItems().length, equalTo(2));
            assertThat(response.getItems()[0].getResponse().getResult(), equalTo(DocWriteResponse.Result.CREATED));
            assertNull(((SimulateIndexResponse) response.getItems()[0].getResponse()).getException());
            assertThat(response.getItems()[1].getResponse().getResult(), equalTo(DocWriteResponse.Result.CREATED));
            assertThat(
                ((SimulateIndexResponse) response.getItems()[1].getResponse()).getException().getMessage(),
                containsString("mapping set to strict, dynamic introduction of")
            );
        }

        {
            // Now we substitute a "test-component-template" that defines both fields, so we expect no exception:
            BulkRequest bulkRequest = new SimulateBulkRequest(
                Map.of(),
                Map.of(
                    "test-component-template",
                    Map.of(
                        "template",
                        Map.of(
                            "mappings",
                            Map.of(
                                "dynamic",
                                "strict",
                                "properties",
                                Map.of("foo1", Map.of("type", "text"), "foo3", Map.of("type", "text"))
                            )
                        )
                    )
                ),
                Map.of(),
                Map.of()
            );
            bulkRequest.add(indexRequest1);
            bulkRequest.add(indexRequest2);
            BulkResponse response = client().execute(new ActionType<BulkResponse>(SimulateBulkAction.NAME), bulkRequest).actionGet();
            assertThat(response.getItems().length, equalTo(2));
            assertThat(response.getItems()[0].getResponse().getResult(), equalTo(DocWriteResponse.Result.CREATED));
            assertNull(((SimulateIndexResponse) response.getItems()[0].getResponse()).getException());
            assertThat(response.getItems()[1].getResponse().getResult(), equalTo(DocWriteResponse.Result.CREATED));
            assertNull(((SimulateIndexResponse) response.getItems()[1].getResponse()).getException());
        }

        {
            /*
             * Now we substitute a "test-component-template-2" that defines both fields, and an index template that pulls it in, so we
             * expect no exception:
             */
            BulkRequest bulkRequest = new SimulateBulkRequest(
                Map.of(),
                Map.of(
                    "test-component-template-2",
                    Map.of(
                        "template",
                        Map.of(
                            "mappings",
                            Map.of(
                                "dynamic",
                                "strict",
                                "properties",
                                Map.of("foo1", Map.of("type", "text"), "foo3", Map.of("type", "text"))
                            )
                        )
                    )
                ),
                Map.of(
                    indexTemplateName,
                    Map.of("index_patterns", List.of(indexName), "composed_of", List.of("test-component-template-2"))
                ),
                Map.of()
            );
            bulkRequest.add(indexRequest1);
            bulkRequest.add(indexRequest2);
            BulkResponse response = client().execute(new ActionType<BulkResponse>(SimulateBulkAction.NAME), bulkRequest).actionGet();
            assertThat(response.getItems().length, equalTo(2));
            assertThat(response.getItems()[0].getResponse().getResult(), equalTo(DocWriteResponse.Result.CREATED));
            assertNull(((SimulateIndexResponse) response.getItems()[0].getResponse()).getException());
            assertThat(response.getItems()[1].getResponse().getResult(), equalTo(DocWriteResponse.Result.CREATED));
            assertNull(((SimulateIndexResponse) response.getItems()[1].getResponse()).getException());
        }

        {
            /*
             * Now we mapping_addition that defines both fields, so we expect no exception:
             */
            BulkRequest bulkRequest = new SimulateBulkRequest(
                Map.of(),
                Map.of(),
                Map.of(),
                Map.of(
                    "_doc",
                    Map.of("dynamic", "strict", "properties", Map.of("foo1", Map.of("type", "text"), "foo3", Map.of("type", "text")))
                )
            );
            bulkRequest.add(indexRequest1);
            bulkRequest.add(indexRequest2);
            BulkResponse response = client().execute(new ActionType<BulkResponse>(SimulateBulkAction.NAME), bulkRequest).actionGet();
            assertThat(response.getItems().length, equalTo(2));
            assertThat(response.getItems()[0].getResponse().getResult(), equalTo(DocWriteResponse.Result.CREATED));
            assertNull(((SimulateIndexResponse) response.getItems()[0].getResponse()).getException());
            assertThat(response.getItems()[1].getResponse().getResult(), equalTo(DocWriteResponse.Result.CREATED));
            assertNull(((SimulateIndexResponse) response.getItems()[1].getResponse()).getException());
        }
    }

    public void testMappingValidationIndexDoesNotExistsNoTemplate() {
        /*
         * This test simulates a BulkRequest of two documents into an index that does not exist. There is no template (other than the
         * mapping-less "random-index-template" created by the parent class), so we expect no mapping validation failure.
         */
        String indexName = randomAlphaOfLength(20).toLowerCase(Locale.ROOT);
        BulkRequest bulkRequest = new SimulateBulkRequest(Map.of(), Map.of(), Map.of(), Map.of());
        bulkRequest.add(new IndexRequest(indexName).source("""
            {
              "foo1": "baz"
            }
            """, XContentType.JSON).id(randomUUID()));
        bulkRequest.add(new IndexRequest(indexName).source("""
            {
              "foo3": "baz"
            }
            """, XContentType.JSON).id(randomUUID()));
        BulkResponse response = client().execute(new ActionType<BulkResponse>(SimulateBulkAction.NAME), bulkRequest).actionGet();
        assertThat(response.getItems().length, equalTo(2));
        assertThat(response.getItems()[0].getResponse().getResult(), equalTo(DocWriteResponse.Result.CREATED));
        assertNull(((SimulateIndexResponse) response.getItems()[0].getResponse()).getException());
        assertThat(response.getItems()[1].getResponse().getResult(), equalTo(DocWriteResponse.Result.CREATED));
        assertNull(((SimulateIndexResponse) response.getItems()[1].getResponse()).getException());
    }

    public void testMappingValidationIndexDoesNotExistsV2Template() throws IOException {
        /*
         * This test simulates a BulkRequest of two documents into an index that does not exist. The index matches a v2 index template. It
         * has strict mappings and one of our documents has it as a field not in the mapping, so we expect a mapping validation error.
         */
        String indexName = "my-index-" + randomAlphaOfLength(5).toLowerCase(Locale.ROOT);
        String mappingString = """
            {
              "_doc":{
                "dynamic":"strict",
                "properties":{
                  "foo1":{
                    "type":"text"
                  }
                }
              }
            }
            """;
        CompressedXContent mapping = CompressedXContent.fromJSON(mappingString);
        Template template = new Template(Settings.EMPTY, mapping, null);
        ComposableIndexTemplate composableIndexTemplate = ComposableIndexTemplate.builder()
            .indexPatterns(List.of("my-index-*"))
            .template(template)
            .build();
        TransportPutComposableIndexTemplateAction.Request request = new TransportPutComposableIndexTemplateAction.Request("test");
        request.indexTemplate(composableIndexTemplate);

        client().execute(TransportPutComposableIndexTemplateAction.TYPE, request).actionGet();
        BulkRequest bulkRequest = new SimulateBulkRequest(Map.of(), Map.of(), Map.of(), Map.of());
        bulkRequest.add(new IndexRequest(indexName).source("""
            {
              "foo1": "baz"
            }
            """, XContentType.JSON).id(randomUUID()));
        bulkRequest.add(new IndexRequest(indexName).source("""
            {
              "foo3": "baz"
            }
            """, XContentType.JSON).id(randomUUID()));
        BulkResponse response = client().execute(new ActionType<BulkResponse>(SimulateBulkAction.NAME), bulkRequest).actionGet();
        assertThat(response.getItems().length, equalTo(2));
        assertThat(response.getItems()[0].getResponse().getResult(), equalTo(DocWriteResponse.Result.CREATED));
        assertNull(((SimulateIndexResponse) response.getItems()[0].getResponse()).getException());
        assertThat(response.getItems()[1].getResponse().getResult(), equalTo(DocWriteResponse.Result.CREATED));
        assertThat(
            ((SimulateIndexResponse) response.getItems()[1].getResponse()).getException().getMessage(),
            containsString("mapping set to strict, dynamic introduction of")
        );
    }

    public void testMappingValidationIndexDoesNotExistsV1Template() {
        /*
         * This test simulates a BulkRequest of two documents into an index that does not exist. The index matches a v1 index template. It
         * has a mapping that defines "foo1" as an integer field and one of our documents has it as a string, so we expect a mapping
         * validation exception.
         */
        String indexName = "my-index-" + randomAlphaOfLength(5).toLowerCase(Locale.ROOT);
        indicesAdmin().putTemplate(
            new PutIndexTemplateRequest("test-template").patterns(List.of("my-index-*")).mapping("foo1", "type=integer")
        ).actionGet();
        BulkRequest bulkRequest = new SimulateBulkRequest(Map.of(), Map.of(), Map.of(), Map.of());
        bulkRequest.add(new IndexRequest(indexName).source("""
            {
              "foo1": "baz"
            }
            """, XContentType.JSON).id(randomUUID()));
        bulkRequest.add(new IndexRequest(indexName).source("""
            {
              "foo3": "baz"
            }
            """, XContentType.JSON).id(randomUUID()));
        BulkResponse response = client().execute(new ActionType<BulkResponse>(SimulateBulkAction.NAME), bulkRequest).actionGet();
        assertThat(response.getItems().length, equalTo(2));
        assertThat(response.getItems()[0].getResponse().getResult(), equalTo(DocWriteResponse.Result.CREATED));
        assertThat(
            ((SimulateIndexResponse) response.getItems()[0].getResponse()).getException().getMessage(),
            containsString("failed to parse field [foo1] of type [integer] ")
        );
        assertNull(((SimulateIndexResponse) response.getItems()[1].getResponse()).getException());
        assertThat(response.getItems()[1].getResponse().getResult(), equalTo(DocWriteResponse.Result.CREATED));
    }

    public void testMappingValidationIndexDoesNotExistsDataStream() throws IOException {
        /*
         * This test simulates a BulkRequest of two documents into an index that does not exist. The index matches a v2 index template. It
         * has strict mappings and one of our documents has it as a field not in the mapping, so we expect a mapping validation error.
         */
        String indexName = "my-data-stream-" + randomAlphaOfLength(5).toLowerCase(Locale.ROOT);
        String mappingString = """
            {
              "_doc":{
                "dynamic":"strict",
                "properties":{
                  "foo1":{
                    "type":"text"
                  }
                }
              }
            }
            """;
        CompressedXContent mapping = CompressedXContent.fromJSON(mappingString);
        Template template = new Template(Settings.EMPTY, mapping, null);
        ComposableIndexTemplate.DataStreamTemplate dataStreamTemplate = new ComposableIndexTemplate.DataStreamTemplate();
        ComposableIndexTemplate composableIndexTemplate = ComposableIndexTemplate.builder()
            .indexPatterns(List.of("my-data-stream-*"))
            .dataStreamTemplate(dataStreamTemplate)
            .template(template)
            .build();
        TransportPutComposableIndexTemplateAction.Request request = new TransportPutComposableIndexTemplateAction.Request("test");
        request.indexTemplate(composableIndexTemplate);

        client().execute(TransportPutComposableIndexTemplateAction.TYPE, request).actionGet();
        {
            // First, try with no @timestamp to make sure we're picking up data-stream-specific templates
            BulkRequest bulkRequest = new SimulateBulkRequest(Map.of(), Map.of(), Map.of(), Map.of());
            bulkRequest.add(new IndexRequest(indexName).source("""
                {
                  "foo1": "baz"
                }
                """, XContentType.JSON).id(randomUUID()));
            bulkRequest.add(new IndexRequest(indexName).source("""
                {
                  "foo3": "baz"
                }
                """, XContentType.JSON).id(randomUUID()));
            BulkResponse response = client().execute(new ActionType<BulkResponse>(SimulateBulkAction.NAME), bulkRequest)
                .actionGet(5, TimeUnit.SECONDS);
            assertThat(response.getItems().length, equalTo(2));
            assertThat(response.getItems()[0].getResponse().getResult(), equalTo(DocWriteResponse.Result.CREATED));
            assertThat(
                ((SimulateIndexResponse) response.getItems()[0].getResponse()).getException().getMessage(),
                containsString("data stream timestamp field [@timestamp] is missing")
            );
            assertThat(response.getItems()[1].getResponse().getResult(), equalTo(DocWriteResponse.Result.CREATED));
            assertThat(
                ((SimulateIndexResponse) response.getItems()[1].getResponse()).getException().getMessage(),
                containsString("mapping set to strict, dynamic introduction of")
            );
        }
        {
            // Now with @timestamp
            BulkRequest bulkRequest = new SimulateBulkRequest(Map.of(), Map.of(), Map.of(), Map.of());
            bulkRequest.add(new IndexRequest(indexName).source("""
                {
                  "@timestamp": "2024-08-27",
                  "foo1": "baz"
                }
                """, XContentType.JSON).id(randomUUID()));
            bulkRequest.add(new IndexRequest(indexName).source("""
                {
                  "@timestamp": "2024-08-27",
                  "foo3": "baz"
                }
                """, XContentType.JSON).id(randomUUID()));
            BulkResponse response = client().execute(new ActionType<BulkResponse>(SimulateBulkAction.NAME), bulkRequest).actionGet();
            assertThat(response.getItems().length, equalTo(2));
            assertThat(response.getItems()[0].getResponse().getResult(), equalTo(DocWriteResponse.Result.CREATED));
            assertNull(((SimulateIndexResponse) response.getItems()[0].getResponse()).getException());
            assertThat(response.getItems()[1].getResponse().getResult(), equalTo(DocWriteResponse.Result.CREATED));
            assertThat(
                ((SimulateIndexResponse) response.getItems()[1].getResponse()).getException().getMessage(),
                containsString("mapping set to strict, dynamic introduction of")
            );
        }
    }
}
