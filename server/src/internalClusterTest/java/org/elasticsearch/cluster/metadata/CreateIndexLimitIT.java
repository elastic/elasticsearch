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
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.hamcrest.Matchers.endsWith;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 1)
public class CreateIndexLimitIT extends ESIntegTestCase {

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
                                    ".my-elasticsearch-system-data-stream",
                                    ".test-index-limit-data-stream",
                                    ".test-system-index-exempted"
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

    public void testCreateIndexLimit() throws InterruptedException {
        final String indexName = randomAlphaOfLength(20).toLowerCase(Locale.ROOT);
        int userIndicesLimit = 3;
        final Set<String> systemIndexAndDataStreamsPatterns = Set.of(
            ".my-elasticsearch-system-",
            ".test-index-limit-",
            ".system-index-exempted-"
            // ".ds-.my-elasticsearch-system-data-stream-2026.01.26-00000",
            // ".ds-.test-index-limit-data-stream-2026.01.26-00000",
            // ".ds-.test-system-index-exempted-2026.01.26-00000"
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
