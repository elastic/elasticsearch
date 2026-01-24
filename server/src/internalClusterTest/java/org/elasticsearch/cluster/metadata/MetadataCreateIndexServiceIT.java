/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexClusterStateUpdateRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.support.master.ShardsAcknowledgedResponse;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.indices.ExecutorNames;
import org.elasticsearch.indices.SystemDataStreamDescriptor;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.indices.SystemIndexDescriptorUtils;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SystemIndexPlugin;
import org.elasticsearch.test.ESIntegTestCase;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.matchesPattern;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST)
public class MetadataCreateIndexServiceIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(TestPlugin.class);
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
                        ".test-data-stream",
                        "system data stream test",
                        SystemDataStreamDescriptor.Type.EXTERNAL,
                        ComposableIndexTemplate.builder()
                            .indexPatterns(
                                List.of(
                                    ".my-elasticsearch-system-data-stream-*",
                                    ".test-index-limit-data-stream-*",
                                    ".test-system-index-exempted-*"
                                )
                            )
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
    }

    public void testRequestTemplateIsRespected() throws InterruptedException {
        /*
         * This test passes a template in the CreateIndexClusterStateUpdateRequest, and makes sure that the settings from that template
         * are used when creating the index.
         */
        MetadataCreateIndexService metadataCreateIndexService = internalCluster().getCurrentMasterNodeInstance(
            MetadataCreateIndexService.class
        );
        final String indexName = randomAlphaOfLength(20).toLowerCase(Locale.ROOT);
        final int numberOfReplicas = randomIntBetween(1, 7);
        CreateIndexClusterStateUpdateRequest request = new CreateIndexClusterStateUpdateRequest(
            "testRequestTemplateIsRespected",
            ProjectId.DEFAULT,
            indexName,
            randomAlphaOfLength(20)
        );
        request.setMatchingTemplate(
            ComposableIndexTemplate.builder()
                .template(Template.builder().settings(Settings.builder().put("index.number_of_replicas", numberOfReplicas)))
                .build()
        );
        final CountDownLatch listenerCalledLatch = new CountDownLatch(1);
        ActionListener<ShardsAcknowledgedResponse> listener = new ActionListener<>() {
            @Override
            public void onResponse(ShardsAcknowledgedResponse shardsAcknowledgedResponse) {
                listenerCalledLatch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                logger.error(e);
                listenerCalledLatch.countDown();
            }
        };

        metadataCreateIndexService.createIndex(
            TimeValue.THIRTY_SECONDS,
            TimeValue.THIRTY_SECONDS,
            TimeValue.THIRTY_SECONDS,
            request,
            listener
        );
        listenerCalledLatch.await(10, TimeUnit.SECONDS);
        GetIndexResponse response = admin().indices()
            .getIndex(new GetIndexRequest(TimeValue.THIRTY_SECONDS).indices(indexName))
            .actionGet();
        Settings settings = response.getSettings().get(indexName);
        assertThat(settings.get("index.number_of_replicas"), equalTo(Integer.toString(numberOfReplicas)));
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
            TimeValue.THIRTY_SECONDS,
            TimeValue.THIRTY_SECONDS,
            TimeValue.THIRTY_SECONDS,
            request,
            listener
        );
    }

    private Function<CountDownLatch, ActionListener<ShardsAcknowledgedResponse>> expectSuccessListenerSupplier =
        countDownLatch -> new ActionListener<>() {
            @Override
            public void onResponse(ShardsAcknowledgedResponse shardsAcknowledgedResponse) {
                countDownLatch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                logger.error(e);
                fail(e);
            }
        };

    private Function<CountDownLatch, ActionListener<ShardsAcknowledgedResponse>> expectLimitExceededListenerSupplier =
        countDownLatch -> new ActionListener<>() {
            @Override
            public void onResponse(ShardsAcknowledgedResponse shardsAcknowledgedResponse) {
                fail("This should not succeed");
            }

            @Override
            public void onFailure(Exception e) {
                assertThat(
                    e.getMessage(),
                    matchesPattern(
                            "This action would add an index, but this project currently has \\[\\d*]/\\[\\d*] maximum indices; for more information, "
                            + "see https://www.elastic.co/docs/deploy-manage/production-guidance/optimize-performance/index-count-limit?"
                            + "version=master"
                    )
                );
                countDownLatch.countDown();
            }
        };

    public void testCreateIndexLimit() throws InterruptedException {
        final String indexName = randomAlphaOfLength(20).toLowerCase(Locale.ROOT);
        int userIndicesLimit = randomIntBetween(5, 10);
        final List<String> systemIndexAndDataStreamsPatterns = List.of(
            ".my-elasticsearch-system-",
            ".test-index-limit-",
            ".system-index-exempted-",
            ".my-elasticsearch-system-data-stream-",
            ".test-index-limit-data-stream-",
            ".test-system-index-exempted-"
        );

        final var metadataCreateIndexService = internalCluster().getCurrentMasterNodeInstance(MetadataCreateIndexService.class);
        updateClusterSettings(
            Settings.builder().put(MetadataCreateIndexService.SETTING_CLUSTER_MAX_INDICES_PER_PROJECT.getKey(), userIndicesLimit)
        );

        var systemIndices = userIndicesLimit + randomIntBetween(5, 10);
        var userIndices = userIndicesLimit - 1;
        CountDownLatch listenerCalledLatch = new CountDownLatch(systemIndices);

        // System indices are exempted from limit constraint.
        for (int i = 1; i <= systemIndices; i++) {
            testCreateIndex(
                randomFrom(systemIndexAndDataStreamsPatterns) + i,
                expectSuccessListenerSupplier.apply(listenerCalledLatch),
                metadataCreateIndexService
            );
        }
        // Must wait for system indices creations to complete for IndexMetadata.isSystem to take effects.
        listenerCalledLatch.await(10, TimeUnit.SECONDS);

        listenerCalledLatch = new CountDownLatch(userIndices);
        // System indices are exempted from limit constraint.
        for (int i = 1; i < userIndices; i++) {
            testCreateIndex(indexName + i, expectSuccessListenerSupplier.apply(listenerCalledLatch), metadataCreateIndexService);
        }
        // Within user indices limit should succeed.
        //safeAwait()

        // all subsequent attempts should fail.
        var failedAttempts = randomIntBetween(3, 6);
        listenerCalledLatch = new CountDownLatch(failedAttempts);
        for (int i = 1; i <= failedAttempts; i++) {
            //testCreateIndex(indexName + randomUUID(), expectLimitExceededListenerSupplier.apply(listenerCalledLatch), metadataCreateIndexService);
        }
        //listenerCalledLatch.await(10, TimeUnit.SECONDS);
    }

}
