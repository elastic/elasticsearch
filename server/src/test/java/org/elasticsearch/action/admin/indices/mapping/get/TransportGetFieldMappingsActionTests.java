/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.mapping.get;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.VersionInformation;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.TestIndexNameExpressionResolver;
import org.elasticsearch.snapshots.ShardRestoringException;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpNodeClient;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.junit.After;
import org.junit.Before;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

public class TransportGetFieldMappingsActionTests extends ESTestCase {

    private ThreadPool threadPool;
    private ClusterService clusterService;
    private TransportService transportService;

    @Before
    public void setUpServices() {
        threadPool = new TestThreadPool(getTestName());
        final ProjectMetadata.Builder project = ProjectMetadata.builder(Metadata.DEFAULT_PROJECT_ID);
        for (String index : new String[] { "a", "b" }) {
            project.put(IndexMetadata.builder(index).settings(indexSettings(IndexVersion.current(), randomUUID(), 1, 0)).build(), false);
        }
        clusterService = ClusterServiceUtils.createClusterService(
            ClusterState.builder(new ClusterName(getTestName())).putProjectMetadata(project).build(),
            threadPool
        );
        transportService = MockTransportService.createNewService(
            Settings.EMPTY,
            VersionInformation.CURRENT,
            TransportVersion.current(),
            threadPool
        );
    }

    @After
    public void tearDownServices() throws Exception {
        transportService.close();
        clusterService.close();
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }

    /**
     * Runs the action over indices {@code a} and {@code b}, each of which the per-index action answers as {@code answers} says.
     */
    private PlainActionFuture<GetFieldMappingsResponse> execute(Function<String, Exception> failures) {
        final var client = new NoOpNodeClient(threadPool) {
            @Override
            @SuppressWarnings("unchecked")
            public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> actionType,
                Request request,
                ActionListener<Response> listener
            ) {
                final String index = ((GetFieldMappingsIndexRequest) request).index();
                final Exception failure = failures.apply(index);
                if (failure != null) {
                    listener.onFailure(failure);
                } else {
                    final var mapping = new GetFieldMappingsResponse.FieldMappingMetadata("f", new BytesArray("{}"));
                    listener.onResponse((Response) new GetFieldMappingsResponse(Map.of(index, Map.of("f", mapping))));
                }
            }
        };
        final var action = new TransportGetFieldMappingsAction(
            transportService,
            clusterService,
            new ActionFilters(java.util.Set.of()),
            TestIndexNameExpressionResolver.newInstance(),
            client
        );
        final var future = new PlainActionFuture<GetFieldMappingsResponse>();
        action.execute(null, new GetFieldMappingsRequest().indices("a", "b").fields("f"), future);
        return future;
    }

    private static NoShardAvailableActionException unavailable(String index) {
        return new NoShardAvailableActionException(new ShardId(index, "_na_", 0));
    }

    /**
     * Failed indices are dropped from the response, so when none can answer the response is empty. If that is because the shards are
     * being restored by a restore that reports them, the request fails with the exception that says so.
     */
    public void testFailsWithShardRestoringWhenNoIndexCanAnswerBecauseItIsRestoring() {
        final var restoring = new ShardRestoringException("a", randomUUID());

        final var future = execute(index -> index.equals("a") ? restoring : unavailable(index));

        final var e = expectThrows(ExecutionException.class, () -> future.get(SAFE_AWAIT_TIMEOUT.seconds(), TimeUnit.SECONDS));
        assertThat(e.getCause(), instanceOf(ShardRestoringException.class));
    }

    /**
     * Without a report the response is empty, as it always was.
     */
    public void testResponseIsEmptyWhenNoIndexCanAnswerForAnotherReason() throws Exception {
        final var future = execute(TransportGetFieldMappingsActionTests::unavailable);

        assertThat(future.get(SAFE_AWAIT_TIMEOUT.seconds(), TimeUnit.SECONDS).mappings(), anEmptyMap());
    }

    /**
     * An index that is being restored does not fail a request that another index can answer: it is dropped like any failed index.
     */
    public void testRestoringIndexIsDroppedWhenAnotherIndexAnswers() throws Exception {
        final var restoring = new ShardRestoringException("a", randomUUID());

        final var future = execute(index -> index.equals("a") ? restoring : null);

        final var mappings = future.get(SAFE_AWAIT_TIMEOUT.seconds(), TimeUnit.SECONDS).mappings();
        assertThat(mappings, hasKey("b"));
        assertThat(mappings, not(hasKey("a")));
        assertThat(mappings.size(), equalTo(1));
    }
}
