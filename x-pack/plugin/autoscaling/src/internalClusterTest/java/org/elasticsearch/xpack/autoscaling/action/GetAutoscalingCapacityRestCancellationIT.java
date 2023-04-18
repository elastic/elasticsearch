/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.autoscaling.action;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Cancellable;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.netty4.Netty4Plugin;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.autoscaling.AutoscalingCountTestDeciderService;
import org.elasticsearch.xpack.autoscaling.AutoscalingIntegTestCase;
import org.elasticsearch.xpack.autoscaling.AutoscalingSyncTestDeciderService;
import org.elasticsearch.xpack.autoscaling.LocalStateAutoscaling;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.CancellationException;

import static junit.framework.TestCase.assertTrue;
import static org.elasticsearch.action.support.ActionTestUtils.wrapAsRestResponseListener;
import static org.elasticsearch.common.xcontent.XContentHelper.convertToMap;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class GetAutoscalingCapacityRestCancellationIT extends AutoscalingIntegTestCase {

    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable http
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> result = new ArrayList<>(super.nodePlugins());
        result.add(Netty4Plugin.class);
        return Collections.unmodifiableList(result);
    }

    @Override
    protected boolean ignoreExternalCluster() {
        return true;
    }

    public void testCapacityRestCancellationAndResponse() throws Exception {
        internalCluster().startMasterOnlyNode();

        putAutoscalingPolicy(
            Map.of(
                AutoscalingCountTestDeciderService.NAME,
                Settings.EMPTY,
                AutoscalingSyncTestDeciderService.NAME,
                Settings.builder().put(AutoscalingSyncTestDeciderService.CHECK_FOR_CANCEL.getKey(), randomBoolean()).build()
            )
        );
        try (RestClient restClient = createRestClient()) {

            PlainActionFuture<Response> cancelledFuture = new PlainActionFuture<>();
            PlainActionFuture<Response> successFuture1 = new PlainActionFuture<>();
            PlainActionFuture<Response> successFuture2 = new PlainActionFuture<>();
            Request getCapacityRequest = new Request("GET", "/_autoscaling/capacity");
            Cancellable cancellable = restClient.performRequestAsync(getCapacityRequest, wrapAsRestResponseListener(cancelledFuture));
            LocalStateAutoscaling.AutoscalingTestPlugin plugin = internalCluster().getAnyMasterNodeInstance(PluginsService.class)
                .filterPlugins(LocalStateAutoscaling.class)
                .get(0)
                .testPlugin();
            plugin.syncWithDeciderService(() -> {
                putAutoscalingPolicy(Map.of(AutoscalingCountTestDeciderService.NAME, Settings.EMPTY));
                assertThat(
                    internalCluster().getAnyMasterNodeInstance(TransportGetAutoscalingCapacityAction.class).responseCacheQueueSize(),
                    equalTo(1)
                );
                restClient.performRequestAsync(getCapacityRequest, wrapAsRestResponseListener(successFuture1));
                assertBusy(
                    () -> assertThat(
                        internalCluster().getAnyMasterNodeInstance(TransportGetAutoscalingCapacityAction.class).responseCacheQueueSize(),
                        equalTo(2)
                    )
                );
                restClient.performRequestAsync(getCapacityRequest, wrapAsRestResponseListener(successFuture2));
                assertBusy(
                    () -> assertThat(
                        internalCluster().getAnyMasterNodeInstance(TransportGetAutoscalingCapacityAction.class).responseCacheQueueSize(),
                        equalTo(3)
                    )
                );
                cancellable.cancel();
                waitForCancelledCapacityTask();

                assertFalse(successFuture1.isDone());
                assertFalse(successFuture2.isDone());
            });

            expectThrows(CancellationException.class, cancelledFuture::actionGet);

            Map<String, Object> response1 = responseAsMap(successFuture1.get());
            Map<String, Object> response2 = responseAsMap(successFuture2.get());

            assertThat(response1, equalTo(response2));
            int actualCount = ((Number) XContentMapValues.extractValue("policies.test.deciders.count.reason_details.count", response2))
                .intValue();

            // validates that the cancelled op did not do the count AND that only one of the two successful invocations did actual work.
            assertThat(actualCount, equalTo(1));
        }
    }

    private void waitForCancelledCapacityTask() throws Exception {
        assertBusy(() -> {
            TransportService transportService = internalCluster().getAnyMasterNodeInstance(TransportService.class);
            final TaskManager taskManager = transportService.getTaskManager();
            assertTrue(taskManager.assertCancellableTaskConsistency());
            for (CancellableTask cancellableTask : taskManager.getCancellableTasks().values()) {
                if (cancellableTask.getAction().startsWith(GetAutoscalingCapacityAction.NAME) && cancellableTask.isCancelled()) {
                    return;
                }
            }
            fail("found no cancellable tasks");
        });
    }

    private Map<String, Object> responseAsMap(Response response) throws IOException {
        return convertToMap(XContentType.JSON.xContent(), response.getEntity().getContent(), false);
    }

    private void putAutoscalingPolicy(Map<String, Settings> settingsMap) {
        final PutAutoscalingPolicyAction.Request request1 = new PutAutoscalingPolicyAction.Request(
            "test",
            new TreeSet<>(Set.of(DiscoveryNodeRole.DATA_ROLE.roleName())),
            // test depends on using treemap's internally, i.e., count is evaluated before wait_for_cancel.
            new TreeMap<>(settingsMap)
        );
        final PutAutoscalingPolicyAction.Request request = request1;
        assertAcked(client().execute(PutAutoscalingPolicyAction.INSTANCE, request).actionGet());
    }
}
