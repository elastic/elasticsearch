/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.health;

import org.apache.http.HttpHost;
import org.apache.http.client.methods.HttpGet;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.client.Cancellable;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.health.node.FetchHealthInfoCacheAction;
import org.elasticsearch.health.node.selection.HealthNode;
import org.elasticsearch.http.HttpInfo;
import org.elasticsearch.node.Node;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.netty4.Netty4Plugin;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.action.support.ActionTestUtils.wrapAsRestResponseListener;
import static org.elasticsearch.test.TaskAssertions.assertAllCancellableTasksAreCancelled;
import static org.elasticsearch.test.TaskAssertions.assertAllTasksHaveFinished;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0)
public class GetHealthCancellationIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(getTestTransportPlugin(), MockTransportService.TestPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int ordinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(ordinal, otherSettings))
            .put(NetworkModule.HTTP_DEFAULT_TYPE_SETTING.getKey(), Netty4Plugin.NETTY_HTTP_TRANSPORT_NAME)
            .build();
    }

    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable http
    }

    public void testCancellation() throws Exception {
        internalCluster().startMasterOnlyNode(Settings.builder().put(Node.NODE_NAME_SETTING.getKey(), "master_node").build());
        internalCluster().startDataOnlyNode(Settings.builder().put(Node.NODE_NAME_SETTING.getKey(), "data_node").build());

        final CountDownLatch tasksBlockedLatch = new CountDownLatch(1);
        final SubscribableListener<Void> fetchHealthInfoRequestReleaseListener = new SubscribableListener<>();
        for (TransportService transportService : internalCluster().getInstances(TransportService.class)) {
            ((MockTransportService) transportService).addRequestHandlingBehavior(
                FetchHealthInfoCacheAction.NAME,
                (handler, request, channel, task) -> {
                    tasksBlockedLatch.countDown();
                    fetchHealthInfoRequestReleaseListener.addListener(
                        ActionListener.wrap(ignored -> handler.messageReceived(request, channel, task), e -> {
                            throw new AssertionError("unexpected", e);
                        })
                    );
                }
            );
        }

        final ClusterService clusterService = internalCluster().getCurrentMasterNodeInstance(ClusterService.class);
        final PlainActionFuture<DiscoveryNode> findHealthNodeFuture = new PlainActionFuture<>();
        // the health node might take a bit of time to be assigned by the persistent task framework so we wait until we have a health
        // node in the cluster before proceeding with the test
        // proceeding with the execution before the health node assignment would yield a non-deterministic behaviour as we
        // wouldn't call the transport service anymore (there wouldn't be a node to fetch the health information from)
        final ClusterStateListener clusterStateListener = event -> getHealthNodeIfPresent(event.state(), findHealthNodeFuture);
        clusterService.addListener(clusterStateListener);
        // look up the node in case the health node was assigned before we registered the listener
        getHealthNodeIfPresent(clusterService.state(), findHealthNodeFuture);
        DiscoveryNode healthNode = findHealthNodeFuture.get(10, TimeUnit.SECONDS);
        assert healthNode != null : "the health node must be assigned";
        clusterService.removeListener(clusterStateListener);

        NodesInfoResponse nodesInfoResponse = clusterAdmin().prepareNodesInfo().get();
        for (NodeInfo node : nodesInfoResponse.getNodes()) {
            if (node.getInfo(HttpInfo.class) != null
                && Node.NODE_NAME_SETTING.get(node.getSettings()).equals(healthNode.getName()) == false) {
                // we don't want the request to hit the health node as it will execute it locally (without going through our stub
                // transport service)
                TransportAddress publishAddress = node.getInfo(HttpInfo.class).address().publishAddress();
                InetSocketAddress address = publishAddress.address();
                getRestClient().setNodes(
                    List.of(
                        new org.elasticsearch.client.Node(
                            new HttpHost(NetworkAddress.format(address.getAddress()), address.getPort(), "http")
                        )
                    )
                );
                break;
            }
        }

        final Request request = new Request(HttpGet.METHOD_NAME, "/_health_report");
        final PlainActionFuture<Response> future = new PlainActionFuture<>();
        final Cancellable cancellable = getRestClient().performRequestAsync(request, wrapAsRestResponseListener(future));

        assertFalse(future.isDone());
        safeAwait(tasksBlockedLatch); // must wait for the fetch health info request to start to avoid cancelling being handled earlier
        cancellable.cancel();

        assertAllCancellableTasksAreCancelled(FetchHealthInfoCacheAction.NAME);
        assertAllCancellableTasksAreCancelled(GetHealthAction.NAME);

        fetchHealthInfoRequestReleaseListener.onResponse(null);
        expectThrows(CancellationException.class, future::actionGet);

        assertAllTasksHaveFinished(FetchHealthInfoCacheAction.NAME);
        assertAllTasksHaveFinished(GetHealthAction.NAME);
    }

    private static void getHealthNodeIfPresent(ClusterState event, ActionListener<DiscoveryNode> healthNodeReference) {
        DiscoveryNode healthNode = HealthNode.findHealthNode(event);
        if (healthNode != null) {
            healthNodeReference.onResponse(healthNode);
        }
    }
}
