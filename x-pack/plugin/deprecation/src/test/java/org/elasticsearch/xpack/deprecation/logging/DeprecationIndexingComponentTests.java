/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.deprecation.logging;

import org.apache.logging.log4j.LogManager;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.bulk.TransportBulkAction;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.common.logging.RateLimitingFilter;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleMetadata;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicyMetadata;
import org.elasticsearch.xpack.core.ilm.OperationMode;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.xpack.deprecation.Deprecation.WRITE_DEPRECATION_LOGS_TO_INDEX;
import static org.elasticsearch.xpack.deprecation.logging.DeprecationIndexingComponent.DEPRECATION_INDEXING_FLUSH_INTERVAL;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;

public class DeprecationIndexingComponentTests extends ESTestCase {

    private ThreadPool threadPool;
    private ClusterService clusterService;
    private BulkCountingClient client;

    @BeforeClass
    public static void setNamesForEcsLayout() {
        // NOTE: the node and cluster names are process wide SetOnces, so another suite running in this JVM may have set them already
        try {
            LogConfigurator.setNodeName("test-node");
        } catch (SetOnce.AlreadySetException e) {
            // already set
        }
        try {
            LogConfigurator.setClusterName("test-cluster");
        } catch (SetOnce.AlreadySetException e) {
            // already set
        }
    }

    @Before
    public void setUpComponents() {
        threadPool = new TestThreadPool(getTestName());
        clusterService = ClusterServiceUtils.createClusterService(
            threadPool,
            new ClusterSettings(Settings.EMPTY, Sets.addToCopy(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS, WRITE_DEPRECATION_LOGS_TO_INDEX))
        );
        client = new BulkCountingClient(threadPool);
    }

    @After
    public void tearDownComponents() {
        clusterService.close();
        terminate(threadPool);
    }

    /**
     * A deprecation warning logged before the deprecation template and ILM policy exist is flushed by the bulk processor into the
     * component's buffer, where it waits for indexing to be enabled. If the node stops first, the request is never sent, so stopping
     * must fail it rather than leave closing the component to wait for it.
     */
    public void testCloseDoesNotWaitWhenFlushNeverEnabled() throws Exception {
        final DeprecationIndexingComponent component = createComponent(TimeValue.timeValueMillis(1));
        component.start();
        logDeprecation();
        assertBusy(() -> assertThat(component.pendingRequestsBufferSize(), equalTo(1)));

        assertClosesPromptly(component);
        assertThat(component.pendingRequestsBufferSize(), equalTo(0));
        assertThat(client.bulkRequests.get(), equalTo(0));
    }

    /**
     * Reproduces the hang seen when a node stops before the first flush interval elapses: the batch is still held by the bulk processor,
     * which {@link DeprecationIndexingComponent#close()} flushes only after indexing has already been stopped. Closing the component must
     * not wait for that batch.
     */
    public void testCloseDoesNotWaitForBatchPendingAtStop() {
        final DeprecationIndexingComponent component = createComponent(TimeValue.timeValueHours(1));
        component.start();
        component.clusterChanged(templateAndPolicyInstalledEvent());
        logDeprecation();

        assertClosesPromptly(component);
        assertThat(client.bulkRequests.get(), equalTo(0));
    }

    public void testFlushesDeprecationsOnceTemplateAndPolicyExist() throws Exception {
        final DeprecationIndexingComponent component = createComponent(TimeValue.timeValueMillis(10));
        component.start();
        logDeprecation();
        component.clusterChanged(templateAndPolicyInstalledEvent());

        assertBusy(() -> assertThat(client.bulkRequests.get(), equalTo(1)));
        assertClosesPromptly(component);
    }

    private DeprecationIndexingComponent createComponent(TimeValue flushInterval) {
        return DeprecationIndexingComponent.createDeprecationIndexingComponent(
            client,
            Settings.builder().put(DEPRECATION_INDEXING_FLUSH_INTERVAL.getKey(), flushInterval).build(),
            new RateLimitingFilter(),
            true,
            clusterService
        );
    }

    private void logDeprecation() {
        LogManager.getLogger("org.elasticsearch.deprecation").warn("deprecated in a test");
        assertWarnings("deprecated in a test");
    }

    private static void assertClosesPromptly(DeprecationIndexingComponent component) {
        final long startNanos = System.nanoTime();
        component.close();
        // without completing every buffered bulk listener, close() waits for BulkProcessor2's 30 second close timeout
        assertThat(TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - startNanos), lessThan(25L));
    }

    private static ClusterChangedEvent templateAndPolicyInstalledEvent() {
        final String policyName = ".deprecation-indexing-ilm-policy";
        final ProjectMetadata.Builder project = ProjectMetadata.builder(ProjectId.DEFAULT)
            .put(
                ".deprecation-indexing-template-9",
                ComposableIndexTemplate.builder().indexPatterns(List.of(".logs-elasticsearch.deprecation-*")).build()
            )
            .putCustom(
                IndexLifecycleMetadata.TYPE,
                new IndexLifecycleMetadata(
                    Map.of(policyName, new LifecyclePolicyMetadata(new LifecyclePolicy(policyName, Map.of()), Map.of(), 1, 1)),
                    OperationMode.RUNNING
                )
            );
        final ClusterState state = ClusterState.builder(ClusterName.DEFAULT).putProjectMetadata(project).build();
        return new ClusterChangedEvent("test", state, ClusterState.EMPTY_STATE);
    }

    /**
     * Counts bulk requests and acknowledges them with an empty response, standing in for the node client so the test doesn't need a
     * running node.
     */
    private static class BulkCountingClient extends NoOpClient {
        final AtomicInteger bulkRequests = new AtomicInteger();

        BulkCountingClient(ThreadPool threadPool) {
            super(threadPool);
        }

        @Override
        @SuppressWarnings("unchecked")
        protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
            ActionType<Response> action,
            Request request,
            ActionListener<Response> listener
        ) {
            assertThat(action, equalTo(TransportBulkAction.TYPE));
            bulkRequests.incrementAndGet();
            listener.onResponse((Response) new BulkResponse(new BulkItemResponse[0], 0));
        }
    }
}
