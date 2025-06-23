/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.ingest.SimulateIndexResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.IndexSettingProviders;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.IndexingPressure;
import org.elasticsearch.indices.EmptySystemIndices;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.test.index.IndexVersionUtils;
import org.elasticsearch.test.transport.CapturingTransport;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.After;
import org.junit.Before;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.elasticsearch.test.ClusterServiceUtils.createClusterService;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportSimulateBulkActionTests extends ESTestCase {

    /**
     * Services needed by bulk action
     */
    private TransportService transportService;
    private ClusterService clusterService;
    private TestThreadPool threadPool;
    private IndicesService indicesService;

    private TestTransportSimulateBulkAction bulkAction;

    class TestTransportSimulateBulkAction extends TransportSimulateBulkAction {

        TestTransportSimulateBulkAction() {
            super(
                TransportSimulateBulkActionTests.this.threadPool,
                transportService,
                TransportSimulateBulkActionTests.this.clusterService,
                null,
                new ActionFilters(Set.of()),
                new IndexingPressure(Settings.EMPTY),
                EmptySystemIndices.INSTANCE,
                TestProjectResolvers.DEFAULT_PROJECT_ONLY,
                indicesService,
                NamedXContentRegistry.EMPTY,
                new IndexSettingProviders(Set.of())
            );
        }
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getClass().getName());
        DiscoveryNode discoveryNode = DiscoveryNodeUtils.builder("node")
            .version(
                VersionUtils.randomCompatibleVersion(random(), Version.CURRENT),
                IndexVersions.MINIMUM_COMPATIBLE,
                IndexVersionUtils.randomCompatibleVersion(random())
            )
            .build();
        clusterService = createClusterService(threadPool, discoveryNode);
        CapturingTransport capturingTransport = new CapturingTransport();
        transportService = capturingTransport.createTransportService(
            clusterService.getSettings(),
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            boundAddress -> clusterService.localNode(),
            null,
            Collections.emptySet()
        );
        transportService.start();
        transportService.acceptIncomingRequests();
        indicesService = mock(IndicesService.class);
        bulkAction = new TestTransportSimulateBulkAction();
    }

    @After
    public void tearDown() throws Exception {
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        threadPool = null;
        clusterService.close();
        super.tearDown();
    }

    public void testIndexData() throws IOException {
        Task task = mock(Task.class); // unused
        BulkRequest bulkRequest = new SimulateBulkRequest(Map.of(), Map.of(), Map.of(), Map.of());
        int bulkItemCount = randomIntBetween(0, 200);
        for (int i = 0; i < bulkItemCount; i++) {
            Map<String, ?> source = Map.of(randomAlphaOfLength(10), randomAlphaOfLength(5));
            IndexRequest indexRequest = new IndexRequest(randomAlphaOfLength(10)).id(randomAlphaOfLength(10)).source(source);
            indexRequest.setListExecutedPipelines(true);
            for (int j = 0; j < randomIntBetween(0, 10); j++) {
                indexRequest.addPipeline(randomAlphaOfLength(12));
            }
            bulkRequest.add(indexRequest);
        }
        AtomicBoolean onResponseCalled = new AtomicBoolean(false);
        ActionListener<BulkResponse> listener = new ActionListener<>() {
            @Override
            public void onResponse(BulkResponse response) {
                onResponseCalled.set(true);
                BulkItemResponse[] responseItems = response.getItems();
                assertThat(responseItems.length, equalTo(bulkItemCount));
                assertThat(responseItems.length, equalTo(bulkRequest.requests().size()));
                for (int i = 0; i < responseItems.length; i++) {
                    BulkItemResponse responseItem = responseItems[i];
                    IndexRequest indexRequest = (IndexRequest) bulkRequest.requests().get(i);
                    assertNull(responseItem.getFailure());
                    assertThat(responseItem.getResponse(), instanceOf(SimulateIndexResponse.class));
                    SimulateIndexResponse simulateIndexResponse = responseItem.getResponse();
                    assertThat(simulateIndexResponse.getIndex(), equalTo(indexRequest.index()));
                    /*
                     * SimulateIndexResponse doesn't have an equals() method, and most of its state is private. So we check that
                     * its toXContent method produces the expected output.
                     */
                    String output = Strings.toString(simulateIndexResponse);
                    try {
                        assertEquals(
                            XContentHelper.stripWhitespace(
                                Strings.format(
                                    """
                                        {
                                          "_id": "%s",
                                          "_index": "%s",
                                          "_version": -3,
                                          "_source": %s,
                                          "executed_pipelines": [%s]
                                        }""",
                                    indexRequest.id(),
                                    indexRequest.index(),
                                    convertMapToJsonString(indexRequest.sourceAsMap()),
                                    indexRequest.getExecutedPipelines()
                                        .stream()
                                        .map(pipeline -> "\"" + pipeline + "\"")
                                        .collect(Collectors.joining(","))
                                )
                            ),
                            output
                        );
                    } catch (IOException e) {
                        fail(e);
                    }
                }
            }

            @Override
            public void onFailure(Exception e) {
                fail(e, "Unexpected error");
            }
        };
        bulkAction.doInternalExecute(task, bulkRequest, r -> fail("executor is unused"), listener, randomLongBetween(0, Long.MAX_VALUE));
        assertThat(onResponseCalled.get(), equalTo(true));
    }

    public void testIndexDataWithValidation() throws IOException {
        /*
         * This test makes sure that we validate mappings. It simulates 7 cases:
         * (1) An indexing request to an index with non-strict mappings, or an index request that is valid with respect to the mappings
         *     (the index is in the cluster state, but our mock indicesService.withTempIndexService() does not throw an exception)
         * (2) An indexing request that is invalid with respect to the mappings (the index is in the cluster state, and our mock
         * indicesService.withTempIndexService() throws an exception)
         * (3) An indexing request to a nonexistent index that matches a V1 template and is valid with respect to the mappings
         * (4) An indexing request to a nonexistent index that matches a V1 template and is invalid with respect to the mappings
         * (5) An indexing request to a nonexistent index that matches a V2 template and is valid with respect to the mappings
         * (6) An indexing request to a nonexistent index that matches a V2 template and is invalid with respect to the mappings
         * (7) An indexing request to a nonexistent index that matches no templates
         */
        Task task = mock(Task.class); // unused
        /*
         * Here we only add a mapping_addition because if there is no mapping at all TransportSimulateBulkAction skips mapping validation
         * altogether, and we need it to run for this test to pass.
         */
        BulkRequest bulkRequest = new SimulateBulkRequest(Map.of(), Map.of(), Map.of(), Map.of("_doc", Map.of("dynamic", "strict")));
        int bulkItemCount = randomIntBetween(0, 200);
        Map<String, IndexMetadata> indicesMap = new HashMap<>();
        Map<String, IndexTemplateMetadata> v1Templates = new HashMap<>();
        Map<String, ComposableIndexTemplate> v2Templates = new HashMap<>();
        Metadata.Builder metadataBuilder = new Metadata.Builder();
        Set<String> indicesWithInvalidMappings = new HashSet<>();
        for (int i = 0; i < bulkItemCount; i++) {
            Map<String, ?> source = Map.of(randomAlphaOfLength(10), randomAlphaOfLength(5));
            IndexRequest indexRequest = new IndexRequest(randomAlphaOfLength(10)).id(randomAlphaOfLength(10)).source(source);
            indexRequest.setListExecutedPipelines(true);
            for (int j = 0; j < randomIntBetween(0, 10); j++) {
                indexRequest.addPipeline(randomAlphaOfLength(12));
            }
            bulkRequest.add(indexRequest);
            // Now we randomly decide what we're going to simulate with requests to this index:
            String indexName = indexRequest.index();
            switch (between(0, 6)) {
                case 0 -> {
                    // Indices that have non-strict mappings, or we're sending valid requests for their mappings
                    indicesMap.put(indexName, newIndexMetadata(indexName));
                }
                case 1 -> {
                    // // Indices that we'll pretend to have sent invalid requests to
                    indicesWithInvalidMappings.add(indexName);
                    indicesMap.put(indexName, newIndexMetadata(indexName));
                }
                case 2 -> {
                    // Index does not exist, but matches a V1 template
                    v1Templates.put(indexName, newV1Template(indexName));
                }
                case 3 -> {
                    // Index does not exist, but matches a V1 template
                    v1Templates.put(indexName, newV1Template(indexName));
                    indicesWithInvalidMappings.add(indexName);
                }
                case 4 -> {
                    // Index does not exist, but matches a V2 template
                    v2Templates.put(indexName, newV2Template(indexName));
                }
                case 5 -> {
                    // Index does not exist, but matches a V2 template
                    v2Templates.put(indexName, newV2Template(indexName));
                    indicesWithInvalidMappings.add(indexName);
                }
                case 6 -> {
                    // Index does not exist, and matches no template
                }
                default -> throw new AssertionError("Illegal branch");
            }
        }
        metadataBuilder.indices(indicesMap);
        metadataBuilder.templates(v1Templates);
        metadataBuilder.indexTemplates(v2Templates);
        ClusterServiceUtils.setState(clusterService, new ClusterState.Builder(clusterService.state()).metadata(metadataBuilder));
        AtomicBoolean onResponseCalled = new AtomicBoolean(false);
        ActionListener<BulkResponse> listener = new ActionListener<>() {
            @Override
            public void onResponse(BulkResponse response) {
                onResponseCalled.set(true);
                BulkItemResponse[] responseItems = response.getItems();
                assertThat(responseItems.length, equalTo(bulkItemCount));
                assertThat(responseItems.length, equalTo(bulkRequest.requests().size()));
                for (int i = 0; i < responseItems.length; i++) {
                    BulkItemResponse responseItem = responseItems[i];
                    IndexRequest indexRequest = (IndexRequest) bulkRequest.requests().get(i);
                    assertNull(responseItem.getFailure());
                    assertThat(responseItem.getResponse(), instanceOf(SimulateIndexResponse.class));
                    SimulateIndexResponse simulateIndexResponse = responseItem.getResponse();
                    assertThat(simulateIndexResponse.getIndex(), equalTo(indexRequest.index()));
                    /*
                     * SimulateIndexResponse doesn't have an equals() method, and most of its state is private. So we check that
                     * its toXContent method produces the expected output.
                     */
                    String output = Strings.toString(simulateIndexResponse);
                    try {
                        String indexName = indexRequest.index();
                        if (indicesWithInvalidMappings.contains(indexName)) {
                            assertEquals(
                                XContentHelper.stripWhitespace(
                                    Strings.format(
                                        """
                                            {
                                              "_id": "%s",
                                              "_index": "%s",
                                              "_version": -3,
                                              "_source": %s,
                                              "executed_pipelines": [%s],
                                              "error":{"type":"exception","reason":"invalid mapping"}
                                            }""",
                                        indexRequest.id(),
                                        indexName,
                                        convertMapToJsonString(indexRequest.sourceAsMap()),
                                        indexRequest.getExecutedPipelines()
                                            .stream()
                                            .map(pipeline -> "\"" + pipeline + "\"")
                                            .collect(Collectors.joining(","))
                                    )
                                ),
                                output
                            );
                        } else {
                            /*
                             * Anything else (a non-existent index, a request to an index without strict mappings, or a valid request)
                             * results in no error being reported.
                             */
                            assertEquals(
                                XContentHelper.stripWhitespace(
                                    Strings.format(
                                        """
                                            {
                                              "_id": "%s",
                                              "_index": "%s",
                                              "_version": -3,
                                              "_source": %s,
                                              "executed_pipelines": [%s]
                                            }""",
                                        indexRequest.id(),
                                        indexName,
                                        convertMapToJsonString(indexRequest.sourceAsMap()),
                                        indexRequest.getExecutedPipelines()
                                            .stream()
                                            .map(pipeline -> "\"" + pipeline + "\"")
                                            .collect(Collectors.joining(","))
                                    )
                                ),
                                output
                            );
                        }
                    } catch (IOException e) {
                        fail(e);
                    }
                }
            }

            @Override
            public void onFailure(Exception e) {
                fail(e, "Unexpected error");
            }
        };
        when(indicesService.withTempIndexService(any(), any())).thenAnswer((Answer<?>) invocation -> {
            IndexMetadata imd = invocation.getArgument(0);
            if (indicesWithInvalidMappings.contains(imd.getIndex().getName())) {
                throw new ElasticsearchException("invalid mapping");
            } else {
                // we don't actually care what is returned, as long as no exception is thrown the request is considered valid:
                return null;
            }
        });
        bulkAction.doInternalExecute(task, bulkRequest, r -> fail("executor is unused"), listener, randomLongBetween(0, Long.MAX_VALUE));
        assertThat(onResponseCalled.get(), equalTo(true));
    }

    private IndexMetadata newIndexMetadata(String indexName) {
        Settings dummyIndexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_INDEX_UUID, UUIDs.randomBase64UUID())
            .build();
        return new IndexMetadata.Builder(indexName).settings(dummyIndexSettings).build();
    }

    private IndexTemplateMetadata newV1Template(String indexName) {
        return new IndexTemplateMetadata.Builder(indexName).patterns(List.of(indexName)).build();
    }

    private ComposableIndexTemplate newV2Template(String indexName) {
        return ComposableIndexTemplate.builder().indexPatterns(List.of(indexName)).build();
    }

    private String convertMapToJsonString(Map<String, ?> map) throws IOException {
        try (XContentBuilder builder = JsonXContent.contentBuilder().map(map)) {
            return BytesReference.bytes(builder).utf8ToString();
        }
    }
}
