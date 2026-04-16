/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import io.netty.handler.codec.http.HttpMethod;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.mapping.put.TransportAutoPutMappingAction;
import org.elasticsearch.action.admin.indices.mapping.put.TransportPutMappingAction;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.RestUtils;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.function.Consumer;

import static org.elasticsearch.action.admin.indices.create.AutoCreateAction.AUTO_CREATE_INDEX_MAX_TIMEOUT_SETTING;
import static org.elasticsearch.cluster.metadata.MetadataCreateIndexService.CREATE_INDEX_MAX_TIMEOUT_SETTING;
import static org.elasticsearch.cluster.metadata.MetadataMappingService.PUT_MAPPING_MAX_TIMEOUT_SETTING;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class CreateIndexTimeoutIT extends ESIntegTestCase {

    public static class TestPlugin extends Plugin {
        @Override
        public List<Setting<?>> getSettings() {
            return CollectionUtils.appendToCopyNoNullElements(
                super.getSettings(),
                AUTO_CREATE_INDEX_MAX_TIMEOUT_SETTING,
                CREATE_INDEX_MAX_TIMEOUT_SETTING,
                PUT_MAPPING_MAX_TIMEOUT_SETTING
            );
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopyNoNullElements(super.nodePlugins(), TestPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(otherSettings)
            .put(AUTO_CREATE_INDEX_MAX_TIMEOUT_SETTING.getKey(), "1ms")
            .put(CREATE_INDEX_MAX_TIMEOUT_SETTING.getKey(), "1ms")
            .put(PUT_MAPPING_MAX_TIMEOUT_SETTING.getKey(), "1ms")
            .build();
    }

    private Releasable withBlockedMasterService(ClusterService masterClusterService) {
        final var barrier = new CyclicBarrier(2);
        masterClusterService.submitUnbatchedStateUpdateTask("blocking task", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                safeAwait(barrier);
                safeAwait(barrier);
                return currentState;
            }

            @Override
            public void onFailure(Exception e) {
                fail(e);
            }
        });
        safeAwait(barrier);
        return () -> safeAwait(barrier);
    }

    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable HTTP
    }

    public void testAutoCreateTimeoutLimit() throws Exception {
        final var masterClusterService = internalCluster().getCurrentMasterNodeInstance(ClusterService.class);
        final var restClient = getRestClient();
        final var indexName = randomIndexName();

        final Consumer<Request> timeoutApplier;
        if (randomBoolean()) {
            timeoutApplier = r -> {};
        } else {
            var timeout = randomFrom(
                MasterNodeRequest.INFINITE_MASTER_NODE_TIMEOUT,
                TimeValue.MINUS_ONE,
                TimeValue.THIRTY_SECONDS,
                TimeValue.MAX_VALUE
            ).getStringRep();
            timeoutApplier = r -> r.addParameter("timeout", timeout);
        }

        try (var ignored = withBlockedMasterService(masterClusterService)) {
            final var request = new Request("PUT", "/" + indexName + "/_bulk");
            request.setJsonEntity("""
                {"index":{}}
                {}
                """);
            timeoutApplier.accept(request);
            final var response = ObjectPath.createFromResponse(restClient.performRequest(request));
            logger.info("--> response={}", response);
            assertTrue(response.evaluate("errors"));
            assertEquals(RestStatus.TOO_MANY_REQUESTS.getStatus(), (int) response.evaluate("items.0.index.status"));
            assertThat(response.evaluate("items.0.index.error.type"), equalTo("process_cluster_event_timeout_exception"));
            assertThat(response.evaluate("items.0.index.error.reason"), containsString("failed to process cluster event"));
        }

        updateClusterSettings(Settings.builder().put(AUTO_CREATE_INDEX_MAX_TIMEOUT_SETTING.getKey(), randomFrom("-1", "60s", "1h")));

        final PlainActionFuture<Response> bulkResponseFuture = new PlainActionFuture<>();
        try (var ignored = withBlockedMasterService(masterClusterService)) {
            final var request = new Request("PUT", "/" + indexName + "/_bulk");
            request.setJsonEntity("""
                {"index":{}}
                {}
                """);
            timeoutApplier.accept(request);
            restClient.performRequestAsync(request, new ResponseListener() {
                @Override
                public void onSuccess(Response response) {
                    bulkResponseFuture.onResponse(response);
                }

                @Override
                public void onFailure(Exception exception) {
                    bulkResponseFuture.onFailure(exception);
                }
            });
            awaitPendingTask(masterClusterService, "auto create [" + indexName + "]");
        }
        safeGet(bulkResponseFuture);

        updateClusterSettings(Settings.builder().putNull(AUTO_CREATE_INDEX_MAX_TIMEOUT_SETTING.getKey()));
    }

    public void testReducePriorities() throws Exception {
        final var masterClusterService = internalCluster().getCurrentMasterNodeInstance(ClusterService.class);
        final var restClient = getRestClient();
        final var indexName = randomIndexName();

        try (var ignored = withBlockedMasterService(masterClusterService)) {
            final var createIndexRequest = new CreateIndexRequest(indexName);
            setArbitraryMasterNodeTimeout(createIndexRequest);
            assertThat(
                asInstanceOf(
                    ProcessClusterEventTimeoutException.class,
                    ExceptionsHelper.unwrapCause(
                        safeAwaitFailure(
                            CreateIndexResponse.class,
                            l -> client().execute(TransportCreateIndexAction.TYPE, createIndexRequest, l)
                        )
                    )
                ).getMessage(),
                allOf(
                    containsString("failed to process cluster event"),
                    containsString("create-index"),
                    containsString(indexName),
                    containsString("within 1ms")
                )
            );

            final var request = new Request("PUT", "/" + indexName);
            request.addParameter(RestUtils.REST_MASTER_TIMEOUT_PARAM, createIndexRequest.masterNodeTimeout().getStringRep());
            request.addParameter("ignore", Integer.toString(RestStatus.TOO_MANY_REQUESTS.getStatus()));
            final var response = restClient.performRequest(request);
            assertThat(response.getStatusLine().getStatusCode(), equalTo(RestStatus.TOO_MANY_REQUESTS.getStatus()));
        }

        updateClusterSettings(Settings.builder().put(CREATE_INDEX_MAX_TIMEOUT_SETTING.getKey(), randomFrom("-1", "60s", "1h")));

        final ActionFuture<CreateIndexResponse> createIndexResponseFuture;
        try (var ignored = withBlockedMasterService(masterClusterService)) {
            final var createIndexRequest = new CreateIndexRequest(indexName);
            setArbitraryMasterNodeTimeout(createIndexRequest);
            createIndexResponseFuture = client().execute(TransportCreateIndexAction.TYPE, createIndexRequest);
            awaitPendingTask(masterClusterService, "create-index [" + indexName + "]");
        }
        safeGet(createIndexResponseFuture);

        try (var ignored = withBlockedMasterService(masterClusterService)) {
            final var putMappingRequest = new PutMappingRequest().setConcreteIndex(
                masterClusterService.state().metadata().getProject(ProjectId.DEFAULT).index(indexName).getIndex()
            ).source("f", "type=keyword");
            setArbitraryMasterNodeTimeout(putMappingRequest);
            assertThat(
                asInstanceOf(
                    ProcessClusterEventTimeoutException.class,
                    ExceptionsHelper.unwrapCause(
                        safeAwaitFailure(
                            AcknowledgedResponse.class,
                            l -> client().execute(
                                randomFrom(TransportPutMappingAction.TYPE, TransportAutoPutMappingAction.TYPE),
                                putMappingRequest,
                                l
                            )
                        )
                    )
                ).getMessage(),
                allOf(
                    containsString("failed to process cluster event"),
                    containsString("put-mapping"),
                    containsString(indexName),
                    containsString("within 1ms")
                )
            );

            final var request = ESRestTestCase.newXContentRequest(
                HttpMethod.PUT,
                "/" + indexName + "/_mapping",
                (b, p) -> b.startObject("properties").startObject("f").field("type", "keyword").endObject().endObject()
            );
            request.addParameter(RestUtils.REST_MASTER_TIMEOUT_PARAM, putMappingRequest.masterNodeTimeout().getStringRep());
            request.addParameter("ignore", Integer.toString(RestStatus.TOO_MANY_REQUESTS.getStatus()));
            final var response = restClient.performRequest(request);
            assertThat(response.getStatusLine().getStatusCode(), equalTo(RestStatus.TOO_MANY_REQUESTS.getStatus()));
        }

        updateClusterSettings(Settings.builder().put(PUT_MAPPING_MAX_TIMEOUT_SETTING.getKey(), randomFrom("-1", "60s", "1h")));

        final ActionFuture<AcknowledgedResponse> putMappingResponseFuture;
        try (var ignored = withBlockedMasterService(masterClusterService)) {
            final var putMappingRequest = new PutMappingRequest().setConcreteIndex(
                masterClusterService.state().metadata().getProject(ProjectId.DEFAULT).index(indexName).getIndex()
            ).source("f", "type=keyword");
            setArbitraryMasterNodeTimeout(putMappingRequest);
            putMappingResponseFuture = client().execute(
                randomFrom(TransportPutMappingAction.TYPE, TransportAutoPutMappingAction.TYPE),
                putMappingRequest
            );
            awaitPendingTask(masterClusterService, "put-mapping [" + indexName + "/");
        }
        safeGet(putMappingResponseFuture);

        updateClusterSettings(
            Settings.builder().putNull(CREATE_INDEX_MAX_TIMEOUT_SETTING.getKey()).putNull(PUT_MAPPING_MAX_TIMEOUT_SETTING.getKey())
        );
    }

    private static void awaitPendingTask(ClusterService masterClusterService, String taskSourcePrefix) {
        assertTrue(
            waitUntil(
                () -> masterClusterService.getMasterService()
                    .pendingTasks()
                    .stream()
                    .anyMatch(pct -> pct.getSource().toString().startsWith(taskSourcePrefix))
            )
        );
    }

    private static void setArbitraryMasterNodeTimeout(MasterNodeRequest<?> masterNodeRequest) {
        if (randomBoolean()) {
            masterNodeRequest.masterNodeTimeout(
                // doesn't matter what timeout we set, we'll always have a 1ms timeout
                randomFrom(
                    MasterNodeRequest.INFINITE_MASTER_NODE_TIMEOUT,
                    TimeValue.MINUS_ONE,
                    TimeValue.ZERO,
                    TimeValue.THIRTY_SECONDS,
                    TimeValue.MAX_VALUE
                )
            );
        }
    }

}
