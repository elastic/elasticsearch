/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.snapshots.features.ResetFeatureStateResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexClusterStateUpdateRequest;
import org.elasticsearch.action.datastreams.DeleteDataStreamAction;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.ShardsAcknowledgedResponse;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.indices.ExecutorNames;
import org.elasticsearch.indices.SystemDataStreamDescriptor;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.indices.SystemIndexDescriptorUtils;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SystemIndexPlugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.transport.netty4.Netty4Plugin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.is;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 1)
public class CreateIndexLimitIT extends ESIntegTestCase {

    @Override
    protected boolean addMockHttpTransport() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(DataStreamsPlugin.class);
        plugins.add(TestPlugin.class);
        return plugins;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(NetworkModule.HTTP_TYPE_KEY, Netty4Plugin.NETTY_HTTP_TRANSPORT_NAME)
            .build();
    }

    public static class TestPlugin extends Plugin implements SystemIndexPlugin {
        @Override
        public List<Setting<?>> getSettings() {
            return CollectionUtils.appendToCopyNoNullElements(
                super.getSettings(),
                MetadataCreateIndexService.SETTING_CLUSTER_MAX_INDICES_PER_PROJECT
            );
        }

        @Override
        public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings settings) {
            return List.of(
                SystemIndexDescriptorUtils.createUnmanaged(".my-elasticsearch-system-*", "my elasticsearch system"),
                SystemIndexDescriptorUtils.createUnmanaged(".test-index-limit-*", ".test index limit"),
                SystemIndexDescriptorUtils.createUnmanaged(".system-index-exempted-*", "system index exempted")
            );
        }

        @Override
        public Collection<SystemDataStreamDescriptor> getSystemDataStreamDescriptors() {
            try {
                CompressedXContent mappings = new CompressedXContent("{\"properties\":{\"name\":{\"type\":\"keyword\"}}}");
                return List.of(
                    new SystemDataStreamDescriptor(
                        ".my-elasticsearch-data-stream",
                        "system data stream test",
                        SystemDataStreamDescriptor.Type.EXTERNAL,
                        ComposableIndexTemplate.builder()
                            .indexPatterns(List.of(".my-test-limit-data-stream"))
                            .template(new Template(Settings.EMPTY, mappings, null))
                            .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
                            .build(),
                        Map.of(),
                        List.of("product"),
                        "product",
                        ExecutorNames.DEFAULT_SYSTEM_DATA_STREAM_THREAD_POOLS
                    ),
                    new SystemDataStreamDescriptor(
                        ".test-limit-index-data-stream",
                        "system data stream test limit",
                        SystemDataStreamDescriptor.Type.EXTERNAL,
                        ComposableIndexTemplate.builder()
                            .indexPatterns(List.of(".test-limit-index-data-stream"))
                            .template(new Template(Settings.EMPTY, mappings, null))
                            .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
                            .build(),
                        Map.of(),
                        List.of("product"),
                        "product",
                        ExecutorNames.DEFAULT_SYSTEM_DATA_STREAM_THREAD_POOLS
                    ),
                    new SystemDataStreamDescriptor(
                        ".test-system-index-exempted",
                        "system data stream test exempted",
                        SystemDataStreamDescriptor.Type.EXTERNAL,
                        ComposableIndexTemplate.builder()
                            .indexPatterns(List.of(".test-system-index-exempted"))
                            .template(new Template(Settings.EMPTY, mappings, null))
                            .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
                            .build(),
                        Map.of(),
                        List.of("product"),
                        "product",
                        ExecutorNames.DEFAULT_SYSTEM_DATA_STREAM_THREAD_POOLS
                    )
                );
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public String getFeatureName() {
            return "Test-User-Indices-Limit";
        }

        @Override
        public String getFeatureDescription() {
            return "Test User Indices Limit";
        }

        @Override
        public void cleanUpFeature(
            ClusterService clusterService,
            ProjectResolver projectResolver,
            Client client,
            TimeValue masterNodeTimeout,
            ActionListener<ResetFeatureStateResponse.ResetFeatureStateStatus> listener
        ) {
            Collection<SystemDataStreamDescriptor> dataStreamDescriptors = getSystemDataStreamDescriptors();
            final DeleteDataStreamAction.Request request = new DeleteDataStreamAction.Request(
                TEST_REQUEST_TIMEOUT,
                dataStreamDescriptors.stream()
                    .map(SystemDataStreamDescriptor::getDataStreamName)
                    .collect(Collectors.toList())
                    .toArray(Strings.EMPTY_ARRAY)
            );
            request.indicesOptions(
                IndicesOptions.builder(request.indicesOptions())
                    .concreteTargetOptions(IndicesOptions.ConcreteTargetOptions.ALLOW_UNAVAILABLE_TARGETS)
                    .build()
            );
            try {
                client.execute(
                    DeleteDataStreamAction.INSTANCE,
                    request,
                    ActionListener.wrap(
                        response -> SystemIndexPlugin.super.cleanUpFeature(
                            clusterService,
                            projectResolver,
                            client,
                            masterNodeTimeout,
                            listener
                        ),
                        e -> {
                            Throwable unwrapped = ExceptionsHelper.unwrapCause(e);
                            if (unwrapped instanceof ResourceNotFoundException) {
                                SystemIndexPlugin.super.cleanUpFeature(
                                    clusterService,
                                    projectResolver,
                                    client,
                                    masterNodeTimeout,
                                    listener
                                );
                            } else {
                                listener.onFailure(e);
                            }
                        }
                    )
                );
            } catch (Exception e) {
                Throwable unwrapped = ExceptionsHelper.unwrapCause(e);
                if (unwrapped instanceof ResourceNotFoundException) {
                    SystemIndexPlugin.super.cleanUpFeature(clusterService, projectResolver, client, masterNodeTimeout, listener);
                } else {
                    listener.onFailure(e);
                }
            }
        }
    }

    private void testCreateIndex(
        String indexName,
        ActionListener<ShardsAcknowledgedResponse> listener,
        MetadataCreateIndexService metadataCreateIndexService
    ) {
        CreateIndexClusterStateUpdateRequest request = new CreateIndexClusterStateUpdateRequest(
            "testCreateIndexLimit",
            ProjectId.DEFAULT,
            indexName,
            randomAlphaOfLength(20)
        );
        request.setMatchingTemplate(
            ComposableIndexTemplate.builder()
                .template(Template.builder().settings(Settings.builder().put("index.number_of_replicas", 1)))
                .build()
        );
        metadataCreateIndexService.createIndex(
            TimeValue.timeValueMinutes(1),
            TimeValue.timeValueMinutes(1),
            TimeValue.timeValueMinutes(1),
            request,
            listener
        );
    }

    private final Function<CountDownLatch, ActionListener<ShardsAcknowledgedResponse>> expectSuccessListenerSupplier =
        countDownLatch -> new ActionListener<>() {
            @Override
            public void onResponse(ShardsAcknowledgedResponse shardsAcknowledgedResponse) {
                countDownLatch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                logger.error(e);
                fail("This should succeed");
            }
        };

    private final Function<CountDownLatch, ActionListener<ShardsAcknowledgedResponse>> expectLimitExceededListenerSupplier =
        countDownLatch -> new ActionListener<>() {
            @Override
            public void onResponse(ShardsAcknowledgedResponse shardsAcknowledgedResponse) {
                fail("This should not succeed");
            }

            @Override
            public void onFailure(Exception e) {
                assertThat(
                    e.getMessage(),
                    endsWith(
                        "see https://www.elastic.co/docs/deploy-manage/production-guidance/optimize-performance/index-count-limit?"
                            + "version=master"
                    )
                );
                countDownLatch.countDown();
            }
        };

    private void verifySystemIndicesBackingDataStreams(String dataStream) throws Exception {
        try (RestClient restClient = createRestClient()) {
            Request putRequest = new Request("PUT", "/_data_stream/" + dataStream);
            putRequest.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("X-elastic-product-origin", "product").build());
            Response putResponse = restClient.performRequest(putRequest);
            assertThat(putResponse.getStatusLine().getStatusCode(), is(200));

            Request index = new Request("POST", "/" + dataStream + "/_doc");
            index.setJsonEntity("{ \"@timestamp\": \"2099-03-08T11:06:07.000Z\", \"name\": \"my-name\" }");
            index.addParameter("refresh", "true");
            index.setOptions(putRequest.getOptions());
            Response response = restClient.performRequest(index);
            assertEquals(201, response.getStatusLine().getStatusCode());
        }
    }

    public void testCreateIndexLimit() throws Exception {
        final String indexName = randomAlphaOfLength(20).toLowerCase(Locale.ROOT);
        int userIndicesLimit = 3;
        final Set<String> systemIndexAndDataStreamsPatterns = Set.of(
            ".my-elasticsearch-system-",
            ".test-index-limit-",
            ".system-index-exempted-"
        );

        final var metadataCreateIndexService = internalCluster().getCurrentMasterNodeInstance(MetadataCreateIndexService.class);
        final var clusterService = internalCluster().clusterService(internalCluster().getMasterName());
        updateClusterSettings(
            Settings.builder().put(MetadataCreateIndexService.SETTING_CLUSTER_MAX_INDICES_PER_PROJECT.getKey(), userIndicesLimit)
        );

        // Case 1: System indices are exempted from index count limit constraint.
        var systemIndices = userIndicesLimit + randomIntBetween(1, 2);
        var countDownLatch = new CountDownLatch(systemIndices);
        for (int i = 1; i <= systemIndices; i++) {
            var systemIndex = randomFrom(systemIndexAndDataStreamsPatterns) + i;
            testCreateIndex(systemIndex, expectSuccessListenerSupplier.apply(countDownLatch), metadataCreateIndexService);
        }
        boolean pass = countDownLatch.await(10, TimeUnit.SECONDS);
        if (pass == false) {
            fail("Case 1 System indices are exempted from index count limit constraint");
        }

        // Verify system indices backing data streams creation proceed unimpeded.
        verifySystemIndicesBackingDataStreams(".my-elasticsearch-data-stream");

        // Case 2: Within index limit, all user index creations should succeed.
        countDownLatch = new CountDownLatch(userIndicesLimit);
        for (int i = 1; i <= userIndicesLimit; i++) {
            // System.out.println("zhubo tang + " + 1);
            var userIndex = indexName + "-" + i;
            testCreateIndex(userIndex, expectSuccessListenerSupplier.apply(countDownLatch), metadataCreateIndexService);
        }
        pass = countDownLatch.await(10, TimeUnit.SECONDS);
        if (pass == false) {
            fail("Case 2: Within index limit, all user index creations should succeed.");
        }

        // Case 3: all subsequent attempts should fail.
        var failedAttempts = randomIntBetween(5, 10);
        countDownLatch = new CountDownLatch(failedAttempts);
        for (int i = 1; i <= failedAttempts; i++) {
            var suffix = userIndicesLimit + i;
            testCreateIndex(
                indexName + "-" + suffix,
                expectLimitExceededListenerSupplier.apply(countDownLatch),
                metadataCreateIndexService
            );
        }
        pass = countDownLatch.await(30, TimeUnit.SECONDS);
        if (pass == false) {
            fail("Case 3: all subsequent attempts should fail.");
        }

        var suffix = clusterService.state()
            .projectState(ProjectId.DEFAULT)
            .metadata()
            .stream()
            .filter(indexMetadata -> systemIndexAndDataStreamsPatterns.stream().anyMatch(indexMetadata.getIndex().getName()::startsWith))
            .count() + 1;

        // Case 4: System indices is never unaffected.
        var someMoreSystemIndices = randomIntBetween(2, 3);
        countDownLatch = new CountDownLatch(someMoreSystemIndices);
        for (int i = 1; i <= someMoreSystemIndices; i++) {
            suffix++;
            var systemIndex = randomFrom(systemIndexAndDataStreamsPatterns) + suffix;
            testCreateIndex(systemIndex, expectSuccessListenerSupplier.apply(countDownLatch), metadataCreateIndexService);
        }
        pass = countDownLatch.await(10, TimeUnit.SECONDS);
        if (pass == false) {
            fail("Case 4: all subsequent attempts should fail.");
        }

        // Verify system data stream is unaffected.
        verifySystemIndicesBackingDataStreams(".test-limit-index-data-stream");
        verifySystemIndicesBackingDataStreams(".test-system-index-exempted");

        var increase = randomIntBetween(5, 10);
        updateClusterSettings(
            Settings.builder().put(MetadataCreateIndexService.SETTING_CLUSTER_MAX_INDICES_PER_PROJECT.getKey(), userIndicesLimit + increase)
        );

        // After increase, further user indices can be created.
        countDownLatch = new CountDownLatch(increase);
        for (int i = 1; i <= increase; i++) {
            var userIndex = indexName + "-" + (userIndicesLimit + i);
            testCreateIndex(userIndex, expectSuccessListenerSupplier.apply(countDownLatch), metadataCreateIndexService);
        }
        countDownLatch.await(30, TimeUnit.SECONDS);

    }

}
