/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.slm;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotAction;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.TriFunction;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ilm.OperationMode;
import org.elasticsearch.xpack.core.scheduler.SchedulerEngine;
import org.elasticsearch.xpack.core.slm.SnapshotLifecycleMetadata;
import org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicy;
import org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicyMetadata;
import org.elasticsearch.xpack.core.slm.history.SnapshotHistoryItem;
import org.elasticsearch.xpack.core.slm.history.SnapshotHistoryStore;

import java.io.IOException;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.startsWith;

public class SnapshotLifecycleTaskTests extends ESTestCase {

    public void testGetSnapMetadata() {
        final String id = randomAlphaOfLength(4);
        final SnapshotLifecyclePolicyMetadata slpm = makePolicyMeta(id);
        final SnapshotLifecycleMetadata meta = new SnapshotLifecycleMetadata(Collections.singletonMap(id, slpm), OperationMode.RUNNING);

        final ClusterState state = ClusterState.builder(new ClusterName("test"))
            .metaData(MetaData.builder()
                .putCustom(SnapshotLifecycleMetadata.TYPE, meta)
                .build())
            .build();

        final Optional<SnapshotLifecyclePolicyMetadata> o =
            SnapshotLifecycleTask.getSnapPolicyMetadata(SnapshotLifecycleService.getJobId(slpm), state);

        assertTrue("the policy metadata should be retrieved from the cluster state", o.isPresent());
        assertThat(o.get(), equalTo(slpm));

        assertFalse(SnapshotLifecycleTask.getSnapPolicyMetadata("bad-jobid", state).isPresent());
    }

    public void testSkipCreatingSnapshotWhenJobDoesNotMatch() {
        final String id = randomAlphaOfLength(4);
        final SnapshotLifecyclePolicyMetadata slpm = makePolicyMeta(id);
        final SnapshotLifecycleMetadata meta = new SnapshotLifecycleMetadata(Collections.singletonMap(id, slpm), OperationMode.RUNNING);

        final ClusterState state = ClusterState.builder(new ClusterName("test"))
            .metaData(MetaData.builder()
                .putCustom(SnapshotLifecycleMetadata.TYPE, meta)
                .build())
            .build();

        final ThreadPool threadPool = new TestThreadPool("test");
        try (ClusterService clusterService = ClusterServiceUtils.createClusterService(state, threadPool);
             VerifyingClient client = new VerifyingClient(threadPool, (a, r, l) -> {
                 fail("should not have tried to take a snapshot");
                 return null;
             })) {
            SnapshotHistoryStore historyStore = new VerifyingHistoryStore(null, ZoneOffset.UTC,
                item -> fail("should not have tried to store an item"));

            SnapshotLifecycleTask task = new SnapshotLifecycleTask(client, clusterService, historyStore);

            // Trigger the event, but since the job name does not match, it should
            // not run the function to create a snapshot
            task.triggered(new SchedulerEngine.Event("nonexistent-job", System.currentTimeMillis(), System.currentTimeMillis()));
        }

        threadPool.shutdownNow();
    }

    public void testCreateSnapshotOnTrigger() {
        final String id = randomAlphaOfLength(4);
        final SnapshotLifecyclePolicyMetadata slpm = makePolicyMeta(id);
        final SnapshotLifecycleMetadata meta = new SnapshotLifecycleMetadata(Collections.singletonMap(id, slpm), OperationMode.RUNNING);

        final ClusterState state = ClusterState.builder(new ClusterName("test"))
            .metaData(MetaData.builder()
                .putCustom(SnapshotLifecycleMetadata.TYPE, meta)
                .build())
            .build();

        final ThreadPool threadPool = new TestThreadPool("test");
        final String createSnapResponse = "{" +
            "  \"snapshot\" : {" +
            "    \"snapshot\" : \"snapshot_1\"," +
            "    \"uuid\" : \"bcP3ClgCSYO_TP7_FCBbBw\"," +
            "    \"version_id\" : " + Version.CURRENT.id + "," +
            "    \"version\" : \"" + Version.CURRENT + "\"," +
            "    \"indices\" : [ ]," +
            "    \"include_global_state\" : true," +
            "    \"state\" : \"SUCCESS\"," +
            "    \"start_time\" : \"2019-03-19T22:19:53.542Z\"," +
            "    \"start_time_in_millis\" : 1553033993542," +
            "    \"end_time\" : \"2019-03-19T22:19:53.567Z\"," +
            "    \"end_time_in_millis\" : 1553033993567," +
            "    \"duration_in_millis\" : 25," +
            "    \"failures\" : [ ]," +
            "    \"shards\" : {" +
            "      \"total\" : 0," +
            "      \"failed\" : 0," +
            "      \"successful\" : 0" +
            "    }" +
            "  }" +
            "}";

        final AtomicBoolean clientCalled = new AtomicBoolean(false);
        final SetOnce<String> snapshotName = new SetOnce<>();
        try (ClusterService clusterService = ClusterServiceUtils.createClusterService(state, threadPool);
             // This verifying client will verify that we correctly invoked
             // client.admin().createSnapshot(...) with the appropriate
             // request. It also returns a mock real response
             VerifyingClient client = new VerifyingClient(threadPool,
                 (action, request, listener) -> {
                     assertFalse(clientCalled.getAndSet(true));
                     assertThat(action, instanceOf(CreateSnapshotAction.class));
                     assertThat(request, instanceOf(CreateSnapshotRequest.class));

                     CreateSnapshotRequest req = (CreateSnapshotRequest) request;

                     SnapshotLifecyclePolicy policy = slpm.getPolicy();
                     assertThat(req.snapshot(), startsWith(policy.getName() + "-"));
                     assertThat(req.repository(), equalTo(policy.getRepository()));
                     snapshotName.set(req.snapshot());
                     if (req.indices().length > 0) {
                         assertThat(Arrays.asList(req.indices()), equalTo(policy.getConfig().get("indices")));
                     }
                     boolean globalState = policy.getConfig().get("include_global_state") == null ||
                         Boolean.parseBoolean((String) policy.getConfig().get("include_global_state"));
                     assertThat(req.includeGlobalState(), equalTo(globalState));

                     try {
                         return CreateSnapshotResponse.fromXContent(createParser(JsonXContent.jsonXContent, createSnapResponse));
                     } catch (IOException e) {
                         fail("failed to parse snapshot response");
                         return null;
                     }
                 })) {
            final AtomicBoolean historyStoreCalled = new AtomicBoolean(false);
            SnapshotHistoryStore historyStore = new VerifyingHistoryStore(null, ZoneOffset.UTC,
                item -> {
                    assertFalse(historyStoreCalled.getAndSet(true));
                    final SnapshotLifecyclePolicy policy = slpm.getPolicy();
                    assertEquals(policy.getId(), item.getPolicyId());
                    assertEquals(policy.getRepository(), item.getRepository());
                    assertEquals(policy.getConfig(), item.getSnapshotConfiguration());
                    assertEquals(snapshotName.get(), item.getSnapshotName());
                });

            SnapshotLifecycleTask task = new SnapshotLifecycleTask(client, clusterService, historyStore);
            // Trigger the event with a matching job name for the policy
            task.triggered(new SchedulerEngine.Event(SnapshotLifecycleService.getJobId(slpm),
                System.currentTimeMillis(), System.currentTimeMillis()));

            assertTrue("snapshot should be triggered once", clientCalled.get());
            assertTrue("history store should be called once", historyStoreCalled.get());
        }

        threadPool.shutdownNow();
    }

    /**
     * A client that delegates to a verifying function for action/request/listener
     */
    public static class VerifyingClient extends NoOpClient {

        private final TriFunction<ActionType<?>, ActionRequest, ActionListener<?>, ActionResponse> verifier;

        VerifyingClient(ThreadPool threadPool,
                        TriFunction<ActionType<?>, ActionRequest, ActionListener<?>, ActionResponse> verifier) {
            super(threadPool);
            this.verifier = verifier;
        }

        @Override
        @SuppressWarnings("unchecked")
        protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(ActionType<Response> action,
                                                                                                  Request request,
                                                                                                  ActionListener<Response> listener) {
            listener.onResponse((Response) verifier.apply(action, request, listener));
        }
    }

    private SnapshotLifecyclePolicyMetadata makePolicyMeta(final String id) {
        SnapshotLifecyclePolicy policy = SnapshotLifecycleServiceTests.createPolicy(id);
        Map<String, String> headers = new HashMap<>();
        headers.put("X-Opaque-ID", randomAlphaOfLength(4));
        return SnapshotLifecyclePolicyMetadata.builder()
            .setPolicy(policy)
            .setHeaders(headers)
            .setVersion(1)
            .setModifiedDate(1)
            .build();
    }

    public static class VerifyingHistoryStore extends SnapshotHistoryStore {

        Consumer<SnapshotHistoryItem> verifier;

        public VerifyingHistoryStore(Client client, ZoneId timeZone, Consumer<SnapshotHistoryItem> verifier) {
            super(Settings.EMPTY, client, null);
            this.verifier = verifier;
        }

        @Override
        public void putAsync(SnapshotHistoryItem item) {
            verifier.accept(item);
        }
    }
}
