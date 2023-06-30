/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.health.node.DiskHealthInfo;
import org.elasticsearch.health.node.FetchHealthInfoCacheAction;
import org.elasticsearch.health.node.HealthInfo;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.HealthPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.tracing.Tracer;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import static org.elasticsearch.common.util.CollectionUtils.appendToCopy;
import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST)
public class HealthServiceIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return appendToCopy(super.nodePlugins(), TestHealthPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int ordinal, Settings otherSettings) {
        /*
         * Every once in a while a node tries to publish its health data before it has discovered the health node and gets a
         * NodeNotConnectedException in LocalHealthMonitor. So it waits "health.reporting.local.monitor.interval" and tries again, this
         * time successfully. Lowering that amount of time to the lowest allowed so that this test doesn't take any more time than
         * necessary when that happens.
         */
        return Settings.builder()
            .put(super.nodeSettings(ordinal, otherSettings))
            .put("health.reporting.local.monitor.interval", TimeValue.timeValueSeconds(10))
            .build();
    }

    public void testThatHealthNodeDataIsFetchedAndPassedToIndicators() throws Exception {
        try (InternalTestCluster internalCluster = internalCluster()) {
            ensureStableCluster(internalCluster.getNodeNames().length);
            waitForAllNodesToReportHealth();
            for (String node : internalCluster.getNodeNames()) {
                HealthService healthService = internalCluster.getInstance(HealthService.class, node);
                AtomicBoolean onResponseCalled = new AtomicBoolean(false);
                ActionListener<List<HealthIndicatorResult>> listener = new ActionListener<>() {
                    @Override
                    public void onResponse(List<HealthIndicatorResult> resultList) {
                        /*
                         * The following is really just asserting that the TestHealthIndicatorService's calculate method was called. The
                         * assertions that it actually got the HealthInfo data are in the calculate method of TestHealthIndicatorService.
                         */
                        assertNotNull(resultList);
                        assertThat(resultList.size(), equalTo(1));
                        HealthIndicatorResult testIndicatorResult = resultList.get(0);
                        assertThat(testIndicatorResult.status(), equalTo(HealthStatus.RED));
                        assertThat(testIndicatorResult.symptom(), equalTo(TestHealthIndicatorService.SYMPTOM));
                        onResponseCalled.set(true);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        throw new RuntimeException(e);
                    }
                };
                healthService.getHealth(internalCluster.client(node), TestHealthIndicatorService.NAME, true, 1000, listener);
                assertBusy(() -> assertThat(onResponseCalled.get(), equalTo(true)));
            }
        }
    }

    private void waitForAllNodesToReportHealth() throws Exception {
        assertBusy(() -> {
            ClusterState state = internalCluster().client()
                .admin()
                .cluster()
                .prepareState()
                .clear()
                .setMetadata(true)
                .setNodes(true)
                .get()
                .getState();
            FetchHealthInfoCacheAction.Response healthResponse = internalCluster().client()
                .execute(FetchHealthInfoCacheAction.INSTANCE, new FetchHealthInfoCacheAction.Request())
                .get();
            for (String nodeId : state.getNodes().getNodes().keySet()) {
                assertThat(healthResponse.getHealthInfo().diskInfoByNode().containsKey(nodeId), equalTo(true));
            }
        }, 15, TimeUnit.SECONDS);
    }

    public static final class TestHealthPlugin extends Plugin implements HealthPlugin {

        private final List<HealthIndicatorService> healthIndicatorServices = new ArrayList<>();

        @Override
        public Collection<Object> createComponents(
            Client client,
            ClusterService clusterService,
            ThreadPool threadPool,
            ResourceWatcherService resourceWatcherService,
            ScriptService scriptService,
            NamedXContentRegistry xContentRegistry,
            Environment environment,
            NodeEnvironment nodeEnvironment,
            NamedWriteableRegistry namedWriteableRegistry,
            IndexNameExpressionResolver indexNameExpressionResolver,
            Supplier<RepositoriesService> repositoriesServiceSupplier,
            Tracer tracer,
            AllocationService allocationService,
            IndicesService indicesService
        ) {
            healthIndicatorServices.add(new TestHealthIndicatorService());
            return new ArrayList<>(healthIndicatorServices);
        }

        @Override
        public Collection<HealthIndicatorService> getHealthIndicatorServices() {
            return healthIndicatorServices;
        }
    }

    public static final class TestHealthIndicatorService implements HealthIndicatorService {
        public static final String NAME = "test_indicator";
        public static final String SYMPTOM = "Symptom";

        @Override
        public String name() {
            return NAME;
        }

        @Override
        public HealthIndicatorResult calculate(boolean verbose, int maxAffectedResourcesCount, HealthInfo healthInfo) {
            assertThat(healthInfo.diskInfoByNode().size(), equalTo(internalCluster().getNodeNames().length));
            for (DiskHealthInfo diskHealthInfo : healthInfo.diskInfoByNode().values()) {
                assertThat(diskHealthInfo.healthStatus(), equalTo(HealthStatus.GREEN));
            }
            return createIndicator(HealthStatus.RED, SYMPTOM, HealthIndicatorDetails.EMPTY, List.of(), List.of());
        }
    }
}
