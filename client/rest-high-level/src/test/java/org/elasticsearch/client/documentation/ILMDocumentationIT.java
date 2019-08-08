/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.client.documentation;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryRequest;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsRequest;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsResponse;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.client.ESRestHighLevelClientTestCase;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.AcknowledgedResponse;
import org.elasticsearch.client.ilm.DeleteAction;
import org.elasticsearch.client.ilm.DeleteLifecyclePolicyRequest;
import org.elasticsearch.client.ilm.ExplainLifecycleRequest;
import org.elasticsearch.client.ilm.ExplainLifecycleResponse;
import org.elasticsearch.client.ilm.GetLifecyclePolicyRequest;
import org.elasticsearch.client.ilm.GetLifecyclePolicyResponse;
import org.elasticsearch.client.ilm.IndexLifecycleExplainResponse;
import org.elasticsearch.client.ilm.LifecycleAction;
import org.elasticsearch.client.ilm.LifecycleManagementStatusRequest;
import org.elasticsearch.client.ilm.LifecycleManagementStatusResponse;
import org.elasticsearch.client.ilm.LifecyclePolicy;
import org.elasticsearch.client.ilm.LifecyclePolicyMetadata;
import org.elasticsearch.client.ilm.OperationMode;
import org.elasticsearch.client.ilm.Phase;
import org.elasticsearch.client.ilm.PutLifecyclePolicyRequest;
import org.elasticsearch.client.ilm.RemoveIndexLifecyclePolicyRequest;
import org.elasticsearch.client.ilm.RemoveIndexLifecyclePolicyResponse;
import org.elasticsearch.client.ilm.RetryLifecyclePolicyRequest;
import org.elasticsearch.client.ilm.RolloverAction;
import org.elasticsearch.client.ilm.ShrinkAction;
import org.elasticsearch.client.ilm.StartILMRequest;
import org.elasticsearch.client.ilm.StopILMRequest;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.slm.DeleteSnapshotLifecyclePolicyRequest;
import org.elasticsearch.client.slm.ExecuteSnapshotLifecyclePolicyRequest;
import org.elasticsearch.client.slm.ExecuteSnapshotLifecyclePolicyResponse;
import org.elasticsearch.client.slm.GetSnapshotLifecyclePolicyRequest;
import org.elasticsearch.client.slm.GetSnapshotLifecyclePolicyResponse;
import org.elasticsearch.client.slm.PutSnapshotLifecyclePolicyRequest;
import org.elasticsearch.client.slm.SnapshotInvocationRecord;
import org.elasticsearch.client.slm.SnapshotLifecyclePolicy;
import org.elasticsearch.client.slm.SnapshotLifecyclePolicyMetadata;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotState;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;

public class ILMDocumentationIT extends ESRestHighLevelClientTestCase {

    public void testPutLifecyclePolicy() throws Exception {
        RestHighLevelClient client = highLevelClient();

        // tag::ilm-put-lifecycle-policy-request
        Map<String, Phase> phases = new HashMap<>();
        Map<String, LifecycleAction> hotActions = new HashMap<>();
        hotActions.put(RolloverAction.NAME, new RolloverAction(
                new ByteSizeValue(50, ByteSizeUnit.GB), null, null));
        phases.put("hot", new Phase("hot", TimeValue.ZERO, hotActions)); // <1>

        Map<String, LifecycleAction> deleteActions =
                Collections.singletonMap(DeleteAction.NAME, new DeleteAction());
        phases.put("delete", new Phase("delete",
                new TimeValue(90, TimeUnit.DAYS), deleteActions)); // <2>

        LifecyclePolicy policy = new LifecyclePolicy("my_policy",
                phases); // <3>
        PutLifecyclePolicyRequest request =
                new PutLifecyclePolicyRequest(policy);
        // end::ilm-put-lifecycle-policy-request

        // tag::ilm-put-lifecycle-policy-execute
        AcknowledgedResponse response = client.indexLifecycle().
                putLifecyclePolicy(request, RequestOptions.DEFAULT);
        // end::ilm-put-lifecycle-policy-execute

        // tag::ilm-put-lifecycle-policy-response
        boolean acknowledged = response.isAcknowledged(); // <1>
        // end::ilm-put-lifecycle-policy-response

        assertTrue(acknowledged);

        // Delete the policy so it can be added again
        {
            DeleteLifecyclePolicyRequest deleteRequest =
                    new DeleteLifecyclePolicyRequest("my_policy");
            AcknowledgedResponse deleteResponse = client.indexLifecycle()
                    .deleteLifecyclePolicy(deleteRequest,
                            RequestOptions.DEFAULT);
            assertTrue(deleteResponse.isAcknowledged());
        }

        // tag::ilm-put-lifecycle-policy-execute-listener
        ActionListener<AcknowledgedResponse> listener =
                new ActionListener<AcknowledgedResponse>() {
            @Override
            public void onResponse(AcknowledgedResponse response) {
                boolean acknowledged = response.isAcknowledged(); // <1>
            }

            @Override
            public void onFailure(Exception e) {
                // <2>
            }
        };
        // end::ilm-put-lifecycle-policy-execute-listener

        // Replace the empty listener by a blocking listener in test
        final CountDownLatch latch = new CountDownLatch(1);
        listener = new LatchedActionListener<>(listener, latch);

        // tag::ilm-put-lifecycle-policy-execute-async
        client.indexLifecycle().putLifecyclePolicyAsync(request,
                RequestOptions.DEFAULT, listener); // <1>
        // end::ilm-put-lifecycle-policy-execute-async

        assertTrue(latch.await(30L, TimeUnit.SECONDS));

    }

    public void testDeletePolicy() throws IOException, InterruptedException {
        RestHighLevelClient client = highLevelClient();

        // Set up a policy so we have something to delete
        PutLifecyclePolicyRequest putRequest;
        {
            Map<String, Phase> phases = new HashMap<>();
            Map<String, LifecycleAction> hotActions = new HashMap<>();
            hotActions.put(RolloverAction.NAME, new RolloverAction(
                new ByteSizeValue(50, ByteSizeUnit.GB), null, null));
            phases.put("hot", new Phase("hot", TimeValue.ZERO, hotActions));
            Map<String, LifecycleAction> deleteActions =
                Collections.singletonMap(DeleteAction.NAME,
                    new DeleteAction());
            phases.put("delete",
                new Phase("delete",
                    new TimeValue(90, TimeUnit.DAYS), deleteActions));
            LifecyclePolicy myPolicy = new LifecyclePolicy("my_policy", phases);
            putRequest = new PutLifecyclePolicyRequest(myPolicy);
            AcknowledgedResponse putResponse = client.indexLifecycle().
                putLifecyclePolicy(putRequest, RequestOptions.DEFAULT);
            assertTrue(putResponse.isAcknowledged());
        }

        // tag::ilm-delete-lifecycle-policy-request
        DeleteLifecyclePolicyRequest request =
            new DeleteLifecyclePolicyRequest("my_policy"); // <1>
        // end::ilm-delete-lifecycle-policy-request

        // tag::ilm-delete-lifecycle-policy-execute
        AcknowledgedResponse response = client.indexLifecycle()
            .deleteLifecyclePolicy(request, RequestOptions.DEFAULT);
        // end::ilm-delete-lifecycle-policy-execute

        // tag::ilm-delete-lifecycle-policy-response
        boolean acknowledged = response.isAcknowledged(); // <1>
        // end::ilm-delete-lifecycle-policy-response

        assertTrue(acknowledged);

        // Put the policy again so we can delete it again
        {
            AcknowledgedResponse putResponse = client.indexLifecycle().
                putLifecyclePolicy(putRequest, RequestOptions.DEFAULT);
            assertTrue(putResponse.isAcknowledged());
        }

        // tag::ilm-delete-lifecycle-policy-execute-listener
        ActionListener<AcknowledgedResponse> listener =
            new ActionListener<AcknowledgedResponse>() {
                @Override
                public void onResponse(AcknowledgedResponse response) {
                    boolean acknowledged = response.isAcknowledged(); // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
        // end::ilm-delete-lifecycle-policy-execute-listener

        // Replace the empty listener by a blocking listener in test
        final CountDownLatch latch = new CountDownLatch(1);
        listener = new LatchedActionListener<>(listener, latch);

        // tag::ilm-delete-lifecycle-policy-execute-async
        client.indexLifecycle().deleteLifecyclePolicyAsync(request,
            RequestOptions.DEFAULT, listener); // <1>
        // end::ilm-delete-lifecycle-policy-execute-async

        assertTrue(latch.await(30L, TimeUnit.SECONDS));
    }

    public void testGetLifecyclePolicy() throws IOException, InterruptedException {
        RestHighLevelClient client = highLevelClient();

        LifecyclePolicy myPolicyAsPut;
        LifecyclePolicy otherPolicyAsPut;
        // Set up some policies so we have something to get
        {
            Map<String, Phase> phases = new HashMap<>();
            Map<String, LifecycleAction> hotActions = new HashMap<>();
            hotActions.put(RolloverAction.NAME, new RolloverAction(
                new ByteSizeValue(50, ByteSizeUnit.GB), null, null));
            phases.put("hot", new Phase("hot", TimeValue.ZERO, hotActions));

            Map<String, LifecycleAction> deleteActions =
                Collections.singletonMap(DeleteAction.NAME,
                    new DeleteAction());
            phases.put("delete",
                new Phase("delete",
                    new TimeValue(90, TimeUnit.DAYS), deleteActions));

            myPolicyAsPut = new LifecyclePolicy("my_policy", phases);
            PutLifecyclePolicyRequest putRequest = new PutLifecyclePolicyRequest(myPolicyAsPut);

            Map<String, Phase> otherPolicyPhases = new HashMap<>(phases);
            Map<String, LifecycleAction> warmActions = Collections.singletonMap(ShrinkAction.NAME, new ShrinkAction(1));
            otherPolicyPhases.put("warm", new Phase("warm", new TimeValue(30, TimeUnit.DAYS), warmActions));
            otherPolicyAsPut = new LifecyclePolicy("other_policy", otherPolicyPhases);

            PutLifecyclePolicyRequest putRequest2 = new PutLifecyclePolicyRequest(otherPolicyAsPut);

            AcknowledgedResponse putResponse = client.indexLifecycle().
                putLifecyclePolicy(putRequest, RequestOptions.DEFAULT);
            assertTrue(putResponse.isAcknowledged());
            AcknowledgedResponse putResponse2 = client.indexLifecycle().
                putLifecyclePolicy(putRequest2, RequestOptions.DEFAULT);
            assertTrue(putResponse2.isAcknowledged());
        }

        // tag::ilm-get-lifecycle-policy-request
        GetLifecyclePolicyRequest allRequest =
            new GetLifecyclePolicyRequest(); // <1>
        GetLifecyclePolicyRequest request =
            new GetLifecyclePolicyRequest("my_policy", "other_policy"); // <2>
        // end::ilm-get-lifecycle-policy-request

        // tag::ilm-get-lifecycle-policy-execute
        GetLifecyclePolicyResponse response = client.indexLifecycle()
            .getLifecyclePolicy(request, RequestOptions.DEFAULT);
        // end::ilm-get-lifecycle-policy-execute

        // tag::ilm-get-lifecycle-policy-response
        ImmutableOpenMap<String, LifecyclePolicyMetadata> policies =
            response.getPolicies();
        LifecyclePolicyMetadata myPolicyMetadata =
            policies.get("my_policy"); // <1>
        String myPolicyName = myPolicyMetadata.getName();
        long version = myPolicyMetadata.getVersion();
        String lastModified = myPolicyMetadata.getModifiedDateString();
        long lastModifiedDate = myPolicyMetadata.getModifiedDate();
        LifecyclePolicy myPolicy = myPolicyMetadata.getPolicy(); // <2>
        // end::ilm-get-lifecycle-policy-response

        assertEquals(myPolicyAsPut, myPolicy);
        assertEquals("my_policy", myPolicyName);
        assertNotNull(lastModified);
        assertNotEquals(0, lastModifiedDate);

        LifecyclePolicyMetadata otherPolicyMetadata = policies.get("other_policy");
        assertEquals(otherPolicyAsPut, otherPolicyMetadata.getPolicy());
        assertEquals("other_policy", otherPolicyMetadata.getName());
        assertNotNull(otherPolicyMetadata.getModifiedDateString());
        assertNotEquals(0, otherPolicyMetadata.getModifiedDate());

        // tag::ilm-get-lifecycle-policy-execute-listener
        ActionListener<GetLifecyclePolicyResponse> listener =
            new ActionListener<GetLifecyclePolicyResponse>() {
                @Override
                public void onResponse(GetLifecyclePolicyResponse response)
                {
                    ImmutableOpenMap<String, LifecyclePolicyMetadata>
                        policies = response.getPolicies(); // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
        // end::ilm-get-lifecycle-policy-execute-listener

        // Replace the empty listener by a blocking listener in test
        final CountDownLatch latch = new CountDownLatch(1);
        listener = new LatchedActionListener<>(listener, latch);

        // tag::ilm-get-lifecycle-policy-execute-async
        client.indexLifecycle().getLifecyclePolicyAsync(request,
            RequestOptions.DEFAULT, listener); // <1>
        // end::ilm-get-lifecycle-policy-execute-async

        assertTrue(latch.await(30L, TimeUnit.SECONDS));
    }

    public void testExplainLifecycle() throws Exception {
        RestHighLevelClient client = highLevelClient();

        // create a policy & index
        {
            Map<String, Phase> phases = new HashMap<>();
            Map<String, LifecycleAction> hotActions = new HashMap<>();
            hotActions.put(RolloverAction.NAME, new RolloverAction(
                new ByteSizeValue(50, ByteSizeUnit.GB), null, null));
            phases.put("hot", new Phase("hot", TimeValue.ZERO, hotActions));

            LifecyclePolicy policy = new LifecyclePolicy("my_policy",
                phases);
            PutLifecyclePolicyRequest putRequest =
                new PutLifecyclePolicyRequest(policy);
            client.indexLifecycle().putLifecyclePolicy(putRequest, RequestOptions.DEFAULT);

            CreateIndexRequest createIndexRequest = new CreateIndexRequest("my_index-1")
                .settings(Settings.builder()
                    .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                    .put("index.lifecycle.name", "my_policy")
                    .put("index.lifecycle.rollover_alias", "my_alias")
                    .build());
            createIndexRequest.alias(new Alias("my_alias").writeIndex(true));
            client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
            CreateIndexRequest createOtherIndexRequest = new CreateIndexRequest("other_index")
                .settings(Settings.builder()
                    .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                    .build());
            client.indices().create(createOtherIndexRequest, RequestOptions.DEFAULT);


            // wait for the policy to become active
            assertBusy(() -> assertNotNull(client.indexLifecycle()
                .explainLifecycle(new ExplainLifecycleRequest("my_index-1"), RequestOptions.DEFAULT)
                .getIndexResponses().get("my_index-1").getAction()));
        }

        // tag::ilm-explain-lifecycle-request
        ExplainLifecycleRequest request =
            new ExplainLifecycleRequest("my_index-1", "other_index"); // <1>
        // end::ilm-explain-lifecycle-request


        assertBusy(() -> {
            // tag::ilm-explain-lifecycle-execute
            ExplainLifecycleResponse response = client.indexLifecycle()
                .explainLifecycle(request, RequestOptions.DEFAULT);
            // end::ilm-explain-lifecycle-execute
            assertNotNull(response);

            // tag::ilm-explain-lifecycle-response
            Map<String, IndexLifecycleExplainResponse> indices =
                response.getIndexResponses();
            IndexLifecycleExplainResponse myIndex = indices.get("my_index-1");
            String policyName = myIndex.getPolicyName(); // <1>
            boolean isManaged = myIndex.managedByILM(); // <2>

            String phase = myIndex.getPhase(); // <3>
            long phaseTime = myIndex.getPhaseTime(); // <4>
            String action = myIndex.getAction(); // <5>
            long actionTime = myIndex.getActionTime();
            String step = myIndex.getStep(); // <6>
            long stepTime = myIndex.getStepTime();

            String failedStep = myIndex.getFailedStep(); // <7>
            // end::ilm-explain-lifecycle-response

            assertEquals("my_policy", policyName);
            assertTrue(isManaged);

            assertEquals("hot", phase);
            assertNotEquals(0, phaseTime);
            assertEquals("rollover", action);
            assertNotEquals(0, actionTime);
            assertEquals("check-rollover-ready", step);
            assertNotEquals(0, stepTime);

            assertNull(failedStep);

            IndexLifecycleExplainResponse otherIndex = indices.get("other_index");
            assertFalse(otherIndex.managedByILM());
            assertNull(otherIndex.getPolicyName());
            assertNull(otherIndex.getPhase());
            assertNull(otherIndex.getAction());
            assertNull(otherIndex.getStep());
            assertNull(otherIndex.getFailedStep());
            assertNull(otherIndex.getPhaseExecutionInfo());
            assertNull(otherIndex.getStepInfo());
        });

        // tag::ilm-explain-lifecycle-execute-listener
        ActionListener<ExplainLifecycleResponse> listener =
            new ActionListener<ExplainLifecycleResponse>() {
                @Override
                public void onResponse(ExplainLifecycleResponse response)
                {
                    Map<String, IndexLifecycleExplainResponse> indices =
                        response.getIndexResponses(); // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
        // end::ilm-explain-lifecycle-execute-listener
        final CountDownLatch latch = new CountDownLatch(1);
        listener = new LatchedActionListener<>(listener, latch);

        // tag::ilm-explain-lifecycle-execute-async
        client.indexLifecycle().explainLifecycleAsync(request,
            RequestOptions.DEFAULT, listener); // <1>
        // end::ilm-explain-lifecycle-execute-async
        assertTrue(latch.await(30L, TimeUnit.SECONDS));
    }

    public void testStartStopStatus() throws Exception {
        RestHighLevelClient client = highLevelClient();

        stopILM(client);

        // tag::ilm-status-request
        LifecycleManagementStatusRequest request =
            new LifecycleManagementStatusRequest();
        // end::ilm-status-request

        // Check that ILM has stopped
        {
            // tag::ilm-status-execute
            LifecycleManagementStatusResponse response =
                client.indexLifecycle()
                    .lifecycleManagementStatus(request, RequestOptions.DEFAULT);
            // end::ilm-status-execute

            // tag::ilm-status-response
            OperationMode operationMode = response.getOperationMode(); // <1>
            // end::ilm-status-response

            assertThat(operationMode, Matchers.either(equalTo(OperationMode.STOPPING)).or(equalTo(OperationMode.STOPPED)));
        }

        startILM(client);

        // tag::ilm-status-execute-listener
        ActionListener<LifecycleManagementStatusResponse> listener =
            new ActionListener<LifecycleManagementStatusResponse>() {
                @Override
                public void onResponse(
                        LifecycleManagementStatusResponse response) {
                    OperationMode operationMode = response
                        .getOperationMode(); // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
        // end::ilm-status-execute-listener

        final CountDownLatch latch = new CountDownLatch(1);
        listener = new LatchedActionListener<>(listener, latch);

        // tag::ilm-status-execute-async
        client.indexLifecycle().lifecycleManagementStatusAsync(request,
            RequestOptions.DEFAULT, listener); // <1>
        // end::ilm-status-execute-async
        assertTrue(latch.await(30L, TimeUnit.SECONDS));

        // Check that ILM is running again
        LifecycleManagementStatusResponse response =
            client.indexLifecycle()
                .lifecycleManagementStatus(request, RequestOptions.DEFAULT);

        OperationMode operationMode = response.getOperationMode();
        assertEquals(OperationMode.RUNNING, operationMode);
    }

    private void stopILM(RestHighLevelClient client) throws IOException, InterruptedException {
        // tag::ilm-stop-ilm-request
        StopILMRequest request = new StopILMRequest();
        // end::ilm-stop-ilm-request

        // tag::ilm-stop-ilm-execute
        AcknowledgedResponse response = client.indexLifecycle()
            .stopILM(request, RequestOptions.DEFAULT);
        // end::ilm-stop-ilm-execute

        // tag::ilm-stop-ilm-response
        boolean acknowledged = response.isAcknowledged(); // <1>
        // end::ilm-stop-ilm-response
        assertTrue(acknowledged);

        // tag::ilm-stop-ilm-execute-listener
        ActionListener<AcknowledgedResponse> listener =
            new ActionListener<AcknowledgedResponse>() {
                @Override
                public void onResponse(AcknowledgedResponse response) {
                    boolean acknowledged = response.isAcknowledged(); // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
        // end::ilm-stop-ilm-execute-listener

        // Replace the empty listener by a blocking listener in test
        final CountDownLatch latch = new CountDownLatch(1);
        listener = new LatchedActionListener<>(listener, latch);

        // tag::ilm-stop-ilm-execute-async
        client.indexLifecycle().stopILMAsync(request,
            RequestOptions.DEFAULT, listener); // <1>
        // end::ilm-stop-ilm-execute-async
        assertTrue(latch.await(30L, TimeUnit.SECONDS));
    }

    private void startILM(RestHighLevelClient client) throws IOException, InterruptedException {
        // tag::ilm-start-ilm-request
        StartILMRequest request1 = new StartILMRequest();
        // end::ilm-start-ilm-request

        // tag::ilm-start-ilm-execute
        AcknowledgedResponse response = client.indexLifecycle()
            .startILM(request1, RequestOptions.DEFAULT);
        // end::ilm-start-ilm-execute

        // tag::ilm-start-ilm-response
        boolean acknowledged = response.isAcknowledged(); // <1>
        // end::ilm-start-ilm-response

        assertTrue(acknowledged);

        // tag::ilm-start-ilm-execute-listener
        ActionListener<AcknowledgedResponse> listener =
            new ActionListener<AcknowledgedResponse>() {
                @Override
                public void onResponse(AcknowledgedResponse response) {
                    boolean acknowledged = response.isAcknowledged(); // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
        // end::ilm-start-ilm-execute-listener

        // Replace the empty listener by a blocking listener in test
        final CountDownLatch latch = new CountDownLatch(1);
        listener = new LatchedActionListener<>(listener, latch);

        // tag::ilm-start-ilm-execute-async
        client.indexLifecycle().startILMAsync(request1,
            RequestOptions.DEFAULT, listener); // <1>
        // end::ilm-start-ilm-execute-async

        assertTrue(latch.await(30L, TimeUnit.SECONDS));
    }

    public void testRetryPolicy() throws Exception {
        RestHighLevelClient client = highLevelClient();

        // setup policy to immediately fail on index
        {
            Map<String, Phase> phases = new HashMap<>();
            Map<String, LifecycleAction> warmActions = new HashMap<>();
            warmActions.put(ShrinkAction.NAME, new ShrinkAction(3));
            phases.put("warm", new Phase("warm", TimeValue.ZERO, warmActions));

            LifecyclePolicy policy = new LifecyclePolicy("my_policy",
                phases);
            PutLifecyclePolicyRequest putRequest =
                new PutLifecyclePolicyRequest(policy);
            client.indexLifecycle().putLifecyclePolicy(putRequest, RequestOptions.DEFAULT);

            CreateIndexRequest createIndexRequest = new CreateIndexRequest("my_index")
                .settings(Settings.builder()
                    .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 2)
                    .put("index.lifecycle.name", "my_policy")
                    .build());
            client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
            assertBusy(() -> assertNotNull(client.indexLifecycle()
                .explainLifecycle(new ExplainLifecycleRequest("my_index"), RequestOptions.DEFAULT)
                .getIndexResponses().get("my_index").getFailedStep()));
        }

        // tag::ilm-retry-lifecycle-policy-request
        RetryLifecyclePolicyRequest request =
            new RetryLifecyclePolicyRequest("my_index"); // <1>
        // end::ilm-retry-lifecycle-policy-request


        // tag::ilm-retry-lifecycle-policy-execute
        AcknowledgedResponse response = client.indexLifecycle()
            .retryLifecyclePolicy(request, RequestOptions.DEFAULT);
        // end::ilm-retry-lifecycle-policy-execute

        // tag::ilm-retry-lifecycle-policy-response
        boolean acknowledged = response.isAcknowledged(); // <1>
        // end::ilm-retry-lifecycle-policy-response

        assertTrue(acknowledged);

        // tag::ilm-retry-lifecycle-policy-execute-listener
        ActionListener<AcknowledgedResponse> listener =
            new ActionListener<AcknowledgedResponse>() {
                @Override
                public void onResponse(AcknowledgedResponse response) {
                    boolean acknowledged = response.isAcknowledged(); // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
        // end::ilm-retry-lifecycle-policy-execute-listener

        // Replace the empty listener by a blocking listener in test
        final CountDownLatch latch = new CountDownLatch(1);
        listener = new LatchedActionListener<>(listener, latch);

        // tag::ilm-retry-lifecycle-policy-execute-async
        client.indexLifecycle().retryLifecyclePolicyAsync(request,
            RequestOptions.DEFAULT, listener); // <1>
        // end::ilm-retry-lifecycle-policy-execute-async

        assertTrue(latch.await(30L, TimeUnit.SECONDS));
    }

    public void testRemovePolicyFromIndex() throws Exception {
        RestHighLevelClient client = highLevelClient();

        // setup policy for index
        Map<String, Phase> phases = new HashMap<>();
        phases.put("delete", new Phase("delete", TimeValue.timeValueHours(10L),
            Collections.singletonMap(DeleteAction.NAME, new DeleteAction())));
        LifecyclePolicy policy = new LifecyclePolicy("my_policy", phases);
        PutLifecyclePolicyRequest putRequest = new PutLifecyclePolicyRequest(policy);
        client.indexLifecycle().putLifecyclePolicy(putRequest, RequestOptions.DEFAULT);
        CreateIndexRequest createIndexRequest = new CreateIndexRequest("my_index")
            .settings(Settings.builder()
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put("index.lifecycle.name", "my_policy")
                .build());
        client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
        assertBusy(() -> assertTrue(client.indexLifecycle()
            .explainLifecycle(new ExplainLifecycleRequest("my_index"), RequestOptions.DEFAULT)
            .getIndexResponses().get("my_index").managedByILM()));

        // tag::ilm-remove-lifecycle-policy-from-index-request
        List<String> indices = new ArrayList<>();
        indices.add("my_index");
        RemoveIndexLifecyclePolicyRequest request =
            new RemoveIndexLifecyclePolicyRequest(indices); // <1>
        // end::ilm-remove-lifecycle-policy-from-index-request


        // tag::ilm-remove-lifecycle-policy-from-index-execute
        RemoveIndexLifecyclePolicyResponse response = client
            .indexLifecycle()
            .removeIndexLifecyclePolicy(request, RequestOptions.DEFAULT);
        // end::ilm-remove-lifecycle-policy-from-index-execute

        // tag::ilm-remove-lifecycle-policy-from-index-response
        boolean hasFailures = response.hasFailures(); // <1>
        List<String> failedIndexes = response.getFailedIndexes(); // <2>
        // end::ilm-remove-lifecycle-policy-from-index-response

        {
            assertFalse(hasFailures);
            Map<String, Object> indexSettings = getIndexSettings("my_index");
            assertTrue(Strings.isNullOrEmpty((String) indexSettings.get("index.lifecycle.name")));
        }

        // re-apply policy on index
        updateIndexSettings("my_index", Settings.builder().put("index.lifecycle.name", "my_policy"));
        assertBusy(() -> assertTrue(client.indexLifecycle()
            .explainLifecycle(new ExplainLifecycleRequest("my_index"), RequestOptions.DEFAULT)
            .getIndexResponses().get("my_index").managedByILM()));

        // tag::ilm-remove-lifecycle-policy-from-index-execute-listener
        ActionListener<RemoveIndexLifecyclePolicyResponse> listener =
            new ActionListener<RemoveIndexLifecyclePolicyResponse>() {
                @Override
                public void onResponse(
                        RemoveIndexLifecyclePolicyResponse response) {
                    boolean hasFailures = response.hasFailures(); // <1>
                    List<String> failedIndexes = response.getFailedIndexes();
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
        // end::ilm-remove-lifecycle-policy-from-index-execute-listener

        {
            Map<String, Object> indexSettings = getIndexSettings("my_index");
            assertTrue(Strings.isNullOrEmpty((String) indexSettings.get("index.lifecycle.name")));
        }

        // Replace the empty listener by a blocking listener in test
        final CountDownLatch latch = new CountDownLatch(1);
        listener = new LatchedActionListener<>(listener, latch);

        // tag::ilm-remove-lifecycle-policy-from-index-execute-async
        client.indexLifecycle().removeIndexLifecyclePolicyAsync(request,
            RequestOptions.DEFAULT, listener); // <1>
        // end::ilm-remove-lifecycle-policy-from-index-execute-async

        assertTrue(latch.await(30L, TimeUnit.SECONDS));
    }

    public void testAddSnapshotLifecyclePolicy() throws Exception {
        RestHighLevelClient client = highLevelClient();

        PutRepositoryRequest repoRequest = new PutRepositoryRequest();

        Settings.Builder settingsBuilder = Settings.builder().put("location", ".");
        repoRequest.settings(settingsBuilder);
        repoRequest.name("my_repository");
        repoRequest.type(FsRepository.TYPE);
        org.elasticsearch.action.support.master.AcknowledgedResponse response =
            client.snapshot().createRepository(repoRequest, RequestOptions.DEFAULT);
        assertTrue(response.isAcknowledged());

        //////// PUT
        // tag::slm-put-snapshot-lifecycle-policy
        Map<String, Object> config = new HashMap<>();
        config.put("indices", Collections.singletonList("idx"));
        SnapshotLifecyclePolicy policy = new SnapshotLifecyclePolicy(
            "policy_id", "name", "1 2 3 * * ?", "my_repository", config);
        PutSnapshotLifecyclePolicyRequest request =
            new PutSnapshotLifecyclePolicyRequest(policy);
        // end::slm-put-snapshot-lifecycle-policy

        // tag::slm-put-snapshot-lifecycle-policy-execute
        AcknowledgedResponse resp = client.indexLifecycle()
            .putSnapshotLifecyclePolicy(request, RequestOptions.DEFAULT);
        // end::slm-put-snapshot-lifecycle-policy-execute

        // tag::slm-put-snapshot-lifecycle-policy-response
        boolean putAcknowledged = resp.isAcknowledged(); // <1>
        // end::slm-put-snapshot-lifecycle-policy-response
        assertTrue(putAcknowledged);

        // tag::slm-put-snapshot-lifecycle-policy-execute-listener
        ActionListener<AcknowledgedResponse> putListener =
                new ActionListener<>() {
            @Override
            public void onResponse(AcknowledgedResponse resp) {
                boolean acknowledged = resp.isAcknowledged(); // <1>
            }

            @Override
            public void onFailure(Exception e) {
                // <2>
            }
        };
        // end::slm-put-snapshot-lifecycle-policy-execute-listener

        // tag::slm-put-snapshot-lifecycle-policy-execute-async
        client.indexLifecycle().putSnapshotLifecyclePolicyAsync(request,
            RequestOptions.DEFAULT, putListener);
        // end::slm-put-snapshot-lifecycle-policy-execute-async

        //////// GET
        // tag::slm-get-snapshot-lifecycle-policy
        GetSnapshotLifecyclePolicyRequest getAllRequest =
            new GetSnapshotLifecyclePolicyRequest(); // <1>
        GetSnapshotLifecyclePolicyRequest getRequest =
            new GetSnapshotLifecyclePolicyRequest("policy_id"); // <2>
        // end::slm-get-snapshot-lifecycle-policy

        // tag::slm-get-snapshot-lifecycle-policy-execute
        GetSnapshotLifecyclePolicyResponse getResponse =
            client.indexLifecycle()
                .getSnapshotLifecyclePolicy(getRequest,
                    RequestOptions.DEFAULT);
        // end::slm-get-snapshot-lifecycle-policy-execute

        // tag::slm-get-snapshot-lifecycle-policy-execute-listener
        ActionListener<GetSnapshotLifecyclePolicyResponse> getListener =
                new ActionListener<>() {
            @Override
            public void onResponse(GetSnapshotLifecyclePolicyResponse resp) {
                Map<String, SnapshotLifecyclePolicyMetadata> policies =
                    resp.getPolicies(); // <1>
            }

            @Override
            public void onFailure(Exception e) {
                // <2>
            }
        };
        // end::slm-get-snapshot-lifecycle-policy-execute-listener

        // tag::slm-get-snapshot-lifecycle-policy-execute-async
        client.indexLifecycle().getSnapshotLifecyclePolicyAsync(getRequest,
            RequestOptions.DEFAULT, getListener);
        // end::slm-get-snapshot-lifecycle-policy-execute-async

        assertThat(getResponse.getPolicies().size(), equalTo(1));
        // tag::slm-get-snapshot-lifecycle-policy-response
        SnapshotLifecyclePolicyMetadata policyMeta =
            getResponse.getPolicies().get("policy_id"); // <1>
        long policyVersion = policyMeta.getVersion();
        long policyModificationDate = policyMeta.getModifiedDate();
        long nextPolicyExecutionDate = policyMeta.getNextExecution();
        SnapshotInvocationRecord lastSuccess = policyMeta.getLastSuccess();
        SnapshotInvocationRecord lastFailure = policyMeta.getLastFailure();
        SnapshotLifecyclePolicyMetadata.SnapshotInProgress inProgress =
            policyMeta.getSnapshotInProgress();
        SnapshotLifecyclePolicy retrievedPolicy = policyMeta.getPolicy(); // <2>
        String id = retrievedPolicy.getId();
        String snapshotNameFormat = retrievedPolicy.getName();
        String repositoryName = retrievedPolicy.getRepository();
        String schedule = retrievedPolicy.getSchedule();
        Map<String, Object> snapshotConfiguration = retrievedPolicy.getConfig();
        // end::slm-get-snapshot-lifecycle-policy-response

        assertNotNull(policyMeta);
        assertThat(retrievedPolicy, equalTo(policy));
        assertThat(policyVersion, equalTo(1L));

        createIndex("idx", Settings.builder().put("index.number_of_shards", 1).build());

        //////// EXECUTE
        // tag::slm-execute-snapshot-lifecycle-policy
        ExecuteSnapshotLifecyclePolicyRequest executeRequest =
            new ExecuteSnapshotLifecyclePolicyRequest("policy_id"); // <1>
        // end::slm-execute-snapshot-lifecycle-policy

        // tag::slm-execute-snapshot-lifecycle-policy-execute
        ExecuteSnapshotLifecyclePolicyResponse executeResponse =
            client.indexLifecycle()
                .executeSnapshotLifecyclePolicy(executeRequest,
                    RequestOptions.DEFAULT);
        // end::slm-execute-snapshot-lifecycle-policy-execute

        // tag::slm-execute-snapshot-lifecycle-policy-response
        final String snapshotName = executeResponse.getSnapshotName(); // <1>
        // end::slm-execute-snapshot-lifecycle-policy-response

        assertSnapshotExists(client, "my_repository", snapshotName);

        // tag::slm-execute-snapshot-lifecycle-policy-execute-listener
        ActionListener<ExecuteSnapshotLifecyclePolicyResponse> executeListener =
                new ActionListener<>() {
            @Override
            public void onResponse(ExecuteSnapshotLifecyclePolicyResponse r) {
                String snapshotName = r.getSnapshotName(); // <1>
            }

            @Override
            public void onFailure(Exception e) {
                // <2>
            }
        };
        // end::slm-execute-snapshot-lifecycle-policy-execute-listener

        // We need a listener that will actually wait for the snapshot to be created
        CountDownLatch latch = new CountDownLatch(1);
        executeListener =
            new ActionListener<>() {
                @Override
                public void onResponse(ExecuteSnapshotLifecyclePolicyResponse r) {
                    try {
                        assertSnapshotExists(client, "my_repository", r.getSnapshotName());
                    } catch (Exception e) {
                        // Ignore
                    } finally {
                        latch.countDown();
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    latch.countDown();
                    fail("failed to execute slm execute: " + e);
                }
        };

        // tag::slm-execute-snapshot-lifecycle-policy-execute-async
        client.indexLifecycle()
            .executeSnapshotLifecyclePolicyAsync(executeRequest,
                RequestOptions.DEFAULT, executeListener);
        // end::slm-execute-snapshot-lifecycle-policy-execute-async
        latch.await(5, TimeUnit.SECONDS);

        //////// DELETE
        // tag::slm-delete-snapshot-lifecycle-policy
        DeleteSnapshotLifecyclePolicyRequest deleteRequest =
            new DeleteSnapshotLifecyclePolicyRequest("policy_id"); // <1>
        // end::slm-delete-snapshot-lifecycle-policy

        // tag::slm-delete-snapshot-lifecycle-policy-execute
        AcknowledgedResponse deleteResp = client.indexLifecycle()
            .deleteSnapshotLifecyclePolicy(deleteRequest, RequestOptions.DEFAULT);
        // end::slm-delete-snapshot-lifecycle-policy-execute
        assertTrue(deleteResp.isAcknowledged());

        ActionListener<AcknowledgedResponse> deleteListener = new ActionListener<>() {
            @Override
            public void onResponse(AcknowledgedResponse resp) {
                // no-op
            }

            @Override
            public void onFailure(Exception e) {
                // no-op
            }
        };

        // tag::slm-delete-snapshot-lifecycle-policy-execute-async
        client.indexLifecycle()
            .deleteSnapshotLifecyclePolicyAsync(deleteRequest,
                RequestOptions.DEFAULT, deleteListener);
        // end::slm-delete-snapshot-lifecycle-policy-execute-async

        assertTrue(deleteResp.isAcknowledged());
    }

    private void assertSnapshotExists(final RestHighLevelClient client, final String repo, final String snapshotName) throws Exception {
        assertBusy(() -> {
            GetSnapshotsRequest getSnapshotsRequest = new GetSnapshotsRequest(new String[]{repo}, new String[]{snapshotName});
            try {
                final GetSnapshotsResponse snaps = client.snapshot().get(getSnapshotsRequest, RequestOptions.DEFAULT);
                Optional<SnapshotInfo> info = snaps.getSnapshots(repo).stream().findFirst();
                if (info.isPresent()) {
                    info.ifPresent(si -> {
                        assertThat(si.snapshotId().getName(), equalTo(snapshotName));
                        assertThat(si.state(), equalTo(SnapshotState.SUCCESS));
                    });
                } else {
                    fail("unable to find snapshot; " + snapshotName);
                }
            } catch (Exception e) {
                if (e.getMessage().contains("snapshot_missing_exception")) {
                    fail("snapshot does not exist: " + snapshotName);
                }
                throw e;
            }
        });
    }

    static Map<String, Object> toMap(Response response) throws IOException {
        return XContentHelper.convertToMap(JsonXContent.jsonXContent, EntityUtils.toString(response.getEntity()), false);
    }

}
