/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ResolvedIndexExpression;
import org.elasticsearch.action.ResolvedIndexExpressions;
import org.elasticsearch.action.admin.indices.resolve.ResolveIndexAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.VersionInformation;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.search.SearchResponseMetrics;
import org.elasticsearch.search.SearchResponseUtils;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.crossproject.CrossProjectModeDecider;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteClusterConnectionTests;
import org.elasticsearch.transport.RemoteClusterService;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.net.InetAddress;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.elasticsearch.common.xcontent.XContentHelper.toXContent;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

public class TransportOpenPointInTimeActionTests extends ESTestCase {

    public void testAnUnreachableProjectFailsAStrictOpen() {
        IllegalArgumentException down = new IllegalArgumentException("link down");
        RemoteTransportException wrapped = new RemoteTransportException(
            "node",
            new TransportAddress(InetAddress.getLoopbackAddress(), 9300),
            "indices:admin/resolve/index",
            down
        );
        Exception failure = TransportOpenPointInTimeAction.failureForUnreachableProjects(false, Map.of("P1", wrapped));
        assertThat(failure, instanceOf(ElasticsearchStatusException.class));
        assertThat(((ElasticsearchStatusException) failure).status(), equalTo(RestStatus.BAD_REQUEST));
        assertNull(failure.getCause());
        ElasticsearchException[] rootCauses = ElasticsearchException.guessRootCauses(failure);
        assertThat(rootCauses[0].getMessage(), containsString("[P1]"));
        assertThat(rootCauses[0].getMessage(), containsString("link down"));
        assertEquals(0, failure.getSuppressed().length);
        assertEquals(0, down.getSuppressed().length);
    }

    public void testPartialResultsLeaveAnUnreachableProjectToTheOpen() {
        assertNull(
            TransportOpenPointInTimeAction.failureForUnreachableProjects(true, Map.of("P1", new IllegalStateException("link down")))
        );
    }

    public void testACompleteResolutionDoesNotFail() {
        assertNull(TransportOpenPointInTimeAction.failureForUnreachableProjects(randomBoolean(), Map.of()));
    }

    public void testLaterProjectsAreSuppressedOnTheFirstFailure() throws IOException {
        EsRejectedExecutionException first = new EsRejectedExecutionException("first-link-down");
        IllegalArgumentException second = new IllegalArgumentException("second-link-down");
        Map<String, Exception> remoteFailures = new LinkedHashMap<>();
        remoteFailures.put("b-cluster", second);
        remoteFailures.put("a-cluster", first);
        Exception failure = TransportOpenPointInTimeAction.failureForUnreachableProjects(false, remoteFailures);
        Map<?, ?> error = renderError(failure);
        assertEquals("failed to resolve indices on project [a-cluster]: first-link-down", error.get("reason"));
        List<?> suppressedErrors = (List<?>) error.get("suppressed");
        assertEquals(1, suppressedErrors.size());
        assertEquals(
            "failed to resolve indices on project [b-cluster]: second-link-down",
            ((Map<?, ?>) suppressedErrors.getFirst()).get("reason")
        );
        assertThat(failure, instanceOf(ElasticsearchStatusException.class));
        assertThat(((ElasticsearchStatusException) failure).status(), equalTo(RestStatus.TOO_MANY_REQUESTS));
        assertNull(failure.getCause());
        assertThat(failure.getMessage(), containsString("[a-cluster]"));
        assertThat(failure.getMessage(), containsString("first-link-down"));
        assertThat(failure.getSuppressed(), arrayContaining(instanceOf(ElasticsearchStatusException.class)));
        ElasticsearchStatusException suppressed = (ElasticsearchStatusException) failure.getSuppressed()[0];
        assertThat(suppressed.status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(suppressed.getMessage(), containsString("[b-cluster]"));
        assertThat(suppressed.getMessage(), containsString("second-link-down"));
        assertNull(suppressed.getCause());
        ElasticsearchException[] rootCauses = ElasticsearchException.guessRootCauses(failure);
        assertThat(rootCauses[0].getMessage(), containsString("[a-cluster]"));
        assertThat(rootCauses[0].getMessage(), containsString("first-link-down"));
        assertEquals(0, first.getSuppressed().length);
        assertEquals(0, second.getSuppressed().length);
    }

    public void testTimeoutClassificationSurvivesProjectDecoration() throws IOException {
        Exception timeout = new ElasticsearchTimeoutException("connection timed out");
        if (randomBoolean()) {
            timeout = new RemoteTransportException("resolve failed", timeout);
        }
        Exception failure = TransportOpenPointInTimeAction.failureForUnreachableProjects(false, Map.of("linked", timeout));
        assertThat(failure, instanceOf(ElasticsearchTimeoutException.class));
        assertEquals(RestStatus.TOO_MANY_REQUESTS, ((ElasticsearchException) failure).status());
        Map<?, ?> error = renderError(failure);
        assertEquals("failed to resolve indices on project [linked]: connection timed out", error.get("reason"));
        assertEquals(true, error.get("timed_out"));
    }

    public void testSuppressedTimeoutClassificationSurvivesProjectDecoration() throws IOException {
        Exception failure = TransportOpenPointInTimeAction.failureForUnreachableProjects(
            false,
            Map.of("a", new IllegalArgumentException("bad request"), "b", new ElasticsearchTimeoutException("connection timed out"))
        );
        List<?> suppressed = (List<?>) renderError(failure).get("suppressed");
        assertEquals(1, suppressed.size());
        Map<?, ?> error = (Map<?, ?>) suppressed.getFirst();
        assertEquals("failed to resolve indices on project [b]: connection timed out", error.get("reason"));
        assertEquals(true, error.get("timed_out"));
    }

    public void testStrictOpenFailsWhenLinkedProjectCannotConnect() {
        assertOpenForUnreachableProject(false, false);
    }

    public void testStrictOpenFailsWhenLinkedProjectCannotResolve() {
        assertOpenForUnreachableProject(true, false);
    }

    public void testPartialOpenRetainsUnresolvedProjectAndExclusionOrder() {
        assertOpenForUnreachableProject(randomBoolean(), true);
    }

    private void assertOpenForUnreachableProject(boolean connected, boolean partial) {
        ThreadPool threadPool = new TestThreadPool(getTestName());
        List<DiscoveryNode> linkedNodes = new CopyOnWriteArrayList<>();
        try (
            MockTransportService linked = RemoteClusterConnectionTests.startTransport(
                "linked",
                linkedNodes,
                VersionInformation.CURRENT,
                TransportVersion.current(),
                threadPool
            );
            MockTransportService transportService = MockTransportService.createNewService(
                Settings.builder().putList("cluster.remote.linked.seeds", linked.getLocalNode().getAddress().toString()).build(),
                VersionInformation.CURRENT,
                TransportVersion.current(),
                threadPool
            )
        ) {
            linkedNodes.add(linked.getLocalNode());
            transportService.start();
            transportService.acceptIncomingRequests();
            RemoteClusterService remoteClusterService = transportService.getRemoteClusterService();
            // Keep actual project resolution and transport failures; the shard-opening phase is outside this test.
            SearchTransportService searchTransportService = mock(SearchTransportService.class);
            when(searchTransportService.getRemoteClusterService()).thenReturn(remoteClusterService);
            TransportSearchAction searchAction = mock(TransportSearchAction.class);
            ClusterService clusterService = mock(ClusterService.class);
            when(clusterService.getSettings()).thenReturn(Settings.EMPTY);
            TransportOpenPointInTimeAction action = new TransportOpenPointInTimeAction(
                transportService,
                mock(SearchService.class),
                new ActionFilters(Set.of()),
                searchAction,
                searchTransportService,
                new NamedWriteableRegistry(List.of()),
                clusterService,
                mock(SearchResponseMetrics.class),
                new CrossProjectModeDecider(Settings.builder().put("serverless.cross_project.enabled", true).build())
            );
            IndicesOptions options = IndicesOptions.builder(IndicesOptions.strictExpandOpen())
                .crossProjectModeOptions(new IndicesOptions.CrossProjectModeOptions(true))
                .build();
            OpenPointInTimeRequest request = new OpenPointInTimeRequest("logs-000001", "linked:logs-*", "linked:-logs-test");
            request.keepAlive(TimeValue.timeValueMinutes(1));
            request.indicesOptions(options);
            request.allowPartialSearchResults(partial);
            request.setResolvedIndexExpressions(
                new ResolvedIndexExpressions(
                    List.of(
                        new ResolvedIndexExpression(
                            "logs-*",
                            new ResolvedIndexExpression.LocalExpressions(
                                Set.of("logs-000001"),
                                ResolvedIndexExpression.LocalIndexResolutionResult.SUCCESS
                            ),
                            Set.of("linked:logs-*")
                        )
                    ),
                    null
                )
            );
            Exception down = new ElasticsearchTimeoutException("linked project unavailable");
            linked.registerRequestHandler(
                ResolveIndexAction.REMOTE_TYPE.name(),
                EsExecutors.DIRECT_EXECUTOR_SERVICE,
                ResolveIndexAction.Request::new,
                (resolveRequest, channel, task) -> channel.sendResponse(down)
            );
            if (connected == false) {
                linked.close();
            }

            if (partial) {
                doAnswer(invocation -> {
                    SearchRequest search = invocation.getArgument(1);
                    assertThat(search.indices(), arrayContaining("logs-000001", "linked:logs-*", "linked:-logs-test"));
                    ActionListener<SearchResponse> listener = invocation.getArgument(2);
                    SearchResponse.Cluster skipped = new SearchResponse.Cluster.Builder(
                        new SearchResponse.Cluster("linked", "logs-*,-logs-test", true, null)
                    ).setStatus(SearchResponse.Cluster.Status.SKIPPED).setFailures(List.of(new ShardSearchFailure(down))).build();
                    SearchResponse response = SearchResponseUtils.response()
                        .pointInTimeId(new BytesArray("pit"))
                        .clusters(new SearchResponse.Clusters(Map.of("linked", skipped), false))
                        .build();
                    try {
                        listener.onResponse(response);
                    } finally {
                        response.decRef();
                    }
                    return null;
                }).when(searchAction).executeOpenPit(any(), any(), any(), any());
            }

            PlainActionFuture<OpenPointInTimeResponse> future = new PlainActionFuture<>();
            action.execute(null, request, future);
            if (partial) {
                assertEquals(SearchResponse.Cluster.Status.SKIPPED, future.actionGet().getClusters().getCluster("linked").getStatus());
            } else {
                ElasticsearchException failure = expectThrows(ElasticsearchException.class, future::actionGet);
                assertThat(failure.getMessage(), containsString("failed to resolve indices on project [linked]"));
                if (connected) {
                    assertThat(failure, instanceOf(ElasticsearchTimeoutException.class));
                    assertThat(failure.getMessage(), containsString("linked project unavailable"));
                }
                verifyNoInteractions(searchAction);
            }
        } finally {
            terminate(threadPool);
        }
    }

    private static Map<?, ?> renderError(Exception failure) throws IOException {
        BytesReference rendered = toXContent(
            (builder, params) -> ElasticsearchException.generateFailureXContent(builder, params, failure, true),
            XContentType.JSON,
            randomBoolean()
        );
        return (Map<?, ?>) XContentHelper.convertToMap(rendered, false, XContentType.JSON).v2().get("error");
    }
}
