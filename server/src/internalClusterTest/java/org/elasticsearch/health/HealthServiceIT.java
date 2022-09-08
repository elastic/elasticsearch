/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.health.node.DiskHealthInfo;
import org.elasticsearch.health.node.HealthInfo;
import org.elasticsearch.health.node.HealthInfoCache;
import org.elasticsearch.health.node.selection.HealthNode;
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
import java.util.Map;
import java.util.function.Supplier;

import static org.elasticsearch.common.util.CollectionUtils.appendToCopy;
import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST)
public class HealthServiceIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return appendToCopy(super.nodePlugins(), TestHealthPlugin.class);
    }

    public void testThatHealthNodeDataIsFetchedAndPassedToIndicators() throws Exception {
        try (InternalTestCluster internalCluster = internalCluster()) {
            ensureStableCluster(internalCluster.getNodeNames().length);
            waitForAllNodesToReportHealth();
            for (String node : internalCluster.getNodeNames()) {
                HealthService healthService = internalCluster.getInstance(HealthService.class, node);
                List<HealthIndicatorResult> resultList = healthService.getHealth(
                    internalCluster.client(node),
                    TestHealthIndicatorService.NAME,
                    true
                );
                /*
                 * The following is really just asserting that the TestHealthIndicatorService's calculate method was called. The
                 * assertions that it actually got the HealthInfo data are in the calculate method of TestHealthIndicatorService.
                 */
                assertNotNull(resultList);
                assertThat(resultList.size(), equalTo(1));
                HealthIndicatorResult testIndicatorResult = resultList.get(0);
                assertThat(testIndicatorResult.status(), equalTo(HealthStatus.RED));
                assertThat(testIndicatorResult.symptom(), equalTo(TestHealthIndicatorService.SYMPTOM));
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
            DiscoveryNode healthNode = HealthNode.findHealthNode(state);
            assertNotNull(healthNode);
            Map<String, DiskHealthInfo> healthInfoCache = internalCluster().getInstance(HealthInfoCache.class, healthNode.getName())
                .getHealthInfo()
                .diskInfoByNode();
            assertThat(healthInfoCache.size(), equalTo(state.getNodes().getNodes().keySet().size()));
        });
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
            AllocationDeciders allocationDeciders
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
        public HealthIndicatorResult calculate(boolean explain, HealthInfo healthInfo) {
            assertThat(healthInfo.diskInfoByNode().size(), equalTo(internalCluster().getNodeNames().length));
            for (DiskHealthInfo diskHealthInfo : healthInfo.diskInfoByNode().values()) {
                assertThat(diskHealthInfo.healthStatus(), equalTo(HealthStatus.GREEN));
            }
            return createIndicator(HealthStatus.RED, SYMPTOM, HealthIndicatorDetails.EMPTY, List.of(), List.of());
        }
    }
}
