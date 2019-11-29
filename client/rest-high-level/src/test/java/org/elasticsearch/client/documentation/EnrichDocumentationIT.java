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

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.client.ESRestHighLevelClientTestCase;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.AcknowledgedResponse;
import org.elasticsearch.client.enrich.DeletePolicyRequest;
import org.elasticsearch.client.enrich.ExecutePolicyRequest;
import org.elasticsearch.client.enrich.ExecutePolicyResponse;
import org.elasticsearch.client.enrich.NamedPolicy;
import org.elasticsearch.client.enrich.GetPolicyRequest;
import org.elasticsearch.client.enrich.GetPolicyResponse;
import org.elasticsearch.client.enrich.PutPolicyRequest;
import org.elasticsearch.client.enrich.StatsRequest;
import org.elasticsearch.client.enrich.StatsResponse;
import org.elasticsearch.client.enrich.StatsResponse.CoordinatorStats;
import org.elasticsearch.client.enrich.StatsResponse.ExecutingPolicy;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.junit.After;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class EnrichDocumentationIT extends ESRestHighLevelClientTestCase {

    @After
    public void cleanup() {
        RestHighLevelClient client = highLevelClient();
        DeletePolicyRequest deletePolicyRequest = new DeletePolicyRequest("users-policy");
        try {
            client.enrich().deletePolicy(deletePolicyRequest, RequestOptions.DEFAULT);
        } catch (Exception e) {
            // ignore... it is ok if policy has already been removed
        }
    }

    public void testPutPolicy() throws Exception {
        RestHighLevelClient client = highLevelClient();
        CreateIndexRequest createIndexRequest = new CreateIndexRequest("users")
            .mapping(Map.of("properties", Map.of("email", Map.of("type", "keyword"))));
        client.indices().create(createIndexRequest, RequestOptions.DEFAULT);

        // tag::enrich-put-policy-request
        PutPolicyRequest putPolicyRequest = new PutPolicyRequest(
            "users-policy", "match", List.of("users"),
            "email", List.of("address", "zip", "city", "state"));
        // end::enrich-put-policy-request

        // tag::enrich-put-policy-execute
        AcknowledgedResponse putPolicyResponse =
            client.enrich().putPolicy(putPolicyRequest, RequestOptions.DEFAULT);
        // end::enrich-put-policy-execute

        // tag::enrich-put-policy-response
        boolean isAcknowledged =
            putPolicyResponse.isAcknowledged(); // <1>
        // end::enrich-put-policy-response

        // tag::enrich-put-policy-execute-listener
        ActionListener<AcknowledgedResponse> listener = new ActionListener<>() {
                @Override
                public void onResponse(AcknowledgedResponse response) { // <1>
                    boolean isAcknowledged = response.isAcknowledged();
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
        // end::enrich-put-policy-execute-listener

        // Replace the empty listener by a blocking listener in test
        final CountDownLatch latch = new CountDownLatch(1);
        listener = new LatchedActionListener<>(listener, latch);

        // tag::enrich-put-policy-execute-async
        client.enrich().putPolicyAsync(putPolicyRequest,
            RequestOptions.DEFAULT, listener); // <1>
        // end::enrich-put-policy-execute-async

        assertTrue(latch.await(30L, TimeUnit.SECONDS));
    }

    public void testDeletePolicy() throws Exception {
        RestHighLevelClient client = highLevelClient();

        {
            CreateIndexRequest createIndexRequest = new CreateIndexRequest("users")
                .mapping(Map.of("properties", Map.of("email", Map.of("type", "keyword"))));
            client.indices().create(createIndexRequest, RequestOptions.DEFAULT);

            // Add a policy, so that it can be deleted:
            PutPolicyRequest putPolicyRequest = new PutPolicyRequest(
                "users-policy", "match", List.of("users"),
                "email", List.of("address", "zip", "city", "state"));
            client.enrich().putPolicy(putPolicyRequest, RequestOptions.DEFAULT);
        }

        // tag::enrich-delete-policy-request
        DeletePolicyRequest deletePolicyRequest =
            new DeletePolicyRequest("users-policy");
        // end::enrich-delete-policy-request

        // tag::enrich-delete-policy-execute
        AcknowledgedResponse deletePolicyResponse = client.enrich()
            .deletePolicy(deletePolicyRequest, RequestOptions.DEFAULT);
        // end::enrich-delete-policy-execute

        // tag::enrich-delete-policy-response
        boolean isAcknowledged =
            deletePolicyResponse.isAcknowledged(); // <1>
        // end::enrich-delete-policy-response

        // tag::enrich-delete-policy-execute-listener
        ActionListener<AcknowledgedResponse> listener = new ActionListener<>() {
            @Override
            public void onResponse(AcknowledgedResponse response) { // <1>
                boolean isAcknowledged = response.isAcknowledged();
            }

            @Override
            public void onFailure(Exception e) {
                // <2>
            }
        };
        // end::enrich-delete-policy-execute-listener

        // Replace the empty listener by a blocking listener in test
        final CountDownLatch latch = new CountDownLatch(1);
        listener = new LatchedActionListener<>(listener, latch);

        // tag::enrich-delete-policy-execute-async
        client.enrich().deletePolicyAsync(deletePolicyRequest,
            RequestOptions.DEFAULT, listener); // <1>
        // end::enrich-delete-policy-execute-async

        assertTrue(latch.await(30L, TimeUnit.SECONDS));
    }

    public void testGetPolicy() throws Exception {
        RestHighLevelClient client = highLevelClient();

        CreateIndexRequest createIndexRequest = new CreateIndexRequest("users")
            .mapping(Map.of("properties", Map.of("email", Map.of("type", "keyword"))));
        client.indices().create(createIndexRequest, RequestOptions.DEFAULT);

        PutPolicyRequest putPolicyRequest = new PutPolicyRequest(
            "users-policy", "match", List.of("users"),
            "email", List.of("address", "zip", "city", "state"));
        client.enrich().putPolicy(putPolicyRequest, RequestOptions.DEFAULT);

        // tag::enrich-get-policy-request
        GetPolicyRequest getPolicyRequest = new GetPolicyRequest("users-policy");
        // end::enrich-get-policy-request

        // tag::enrich-get-policy-execute
        GetPolicyResponse getPolicyResponse =
            client.enrich().getPolicy(getPolicyRequest, RequestOptions.DEFAULT);
        // end::enrich-get-policy-execute

        // tag::enrich-get-policy-response
        List<NamedPolicy> policies = getPolicyResponse.getPolicies(); // <1>
        NamedPolicy policy = policies.get(0);
        // end::enrich-get-policy-response

        // tag::enrich-get-policy-execute-listener
        ActionListener<GetPolicyResponse> listener = new ActionListener<>() {
            @Override
            public void onResponse(GetPolicyResponse response) { // <1>
                List<NamedPolicy> policies = response.getPolicies();
                NamedPolicy policy = policies.get(0);
            }

            @Override
            public void onFailure(Exception e) {
                // <2>
            }
        };
        // end::enrich-get-policy-execute-listener

        // Replace the empty listener by a blocking listener in test
        final CountDownLatch latch = new CountDownLatch(1);
        listener = new LatchedActionListener<>(listener, latch);

        // tag::enrich-get-policy-execute-async
        client.enrich().getPolicyAsync(getPolicyRequest,
            RequestOptions.DEFAULT, listener); // <1>
        // end::enrich-get-policy-execute-async

        assertTrue(latch.await(30L, TimeUnit.SECONDS));
    }

    public void testStats() throws Exception {
        RestHighLevelClient client = highLevelClient();

        // tag::enrich-stats-request
        StatsRequest statsRequest = new StatsRequest();
        // end::enrich-stats-request

        // tag::enrich-stats-execute
        StatsResponse statsResponse =
            client.enrich().stats(statsRequest, RequestOptions.DEFAULT);
        // end::enrich-stats-execute

        // tag::enrich-stats-response
        List<ExecutingPolicy> executingPolicies =
            statsResponse.getExecutingPolicies();  // <1>
        List<CoordinatorStats> coordinatorStats =
            statsResponse.getCoordinatorStats();  // <2>
        // end::enrich-stats-response

        // tag::enrich-stats-execute-listener
        ActionListener<StatsResponse> listener = new ActionListener<>() {
            @Override
            public void onResponse(StatsResponse response) { // <1>
                List<ExecutingPolicy> executingPolicies =
                    statsResponse.getExecutingPolicies();
                List<CoordinatorStats> coordinatorStats =
                    statsResponse.getCoordinatorStats();
            }

            @Override
            public void onFailure(Exception e) {
                // <2>
            }
        };
        // end::enrich-stats-execute-listener

        // Replace the empty listener by a blocking listener in test
        final CountDownLatch latch = new CountDownLatch(1);
        listener = new LatchedActionListener<>(listener, latch);

        // tag::enrich-stats-execute-async
        client.enrich().statsAsync(statsRequest, RequestOptions.DEFAULT,
            listener); // <1>
        // end::enrich-stats-execute-async

        assertTrue(latch.await(30L, TimeUnit.SECONDS));
    }

    public void testExecutePolicy() throws Exception {
        RestHighLevelClient client = highLevelClient();

        {
            CreateIndexRequest createIndexRequest = new CreateIndexRequest("users")
                .mapping(Map.of("properties", Map.of("email", Map.of("type", "keyword"))));
            client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
            PutPolicyRequest putPolicyRequest = new PutPolicyRequest(
                "users-policy", "match", List.of("users"),
                "email", List.of("address", "zip", "city", "state"));
            client.enrich().putPolicy(putPolicyRequest, RequestOptions.DEFAULT);
        }

        // tag::enrich-execute-policy-request
        ExecutePolicyRequest request =
            new ExecutePolicyRequest("users-policy");
        // end::enrich-execute-policy-request

        // tag::enrich-execute-policy-execute
        ExecutePolicyResponse response =
            client.enrich().executePolicy(request, RequestOptions.DEFAULT);
        // end::enrich-execute-policy-execute

        // tag::enrich-execute-policy-response
        ExecutePolicyResponse.ExecutionStatus status =
            response.getExecutionStatus();
        // end::enrich-execute-policy-response

        // tag::enrich-execute-policy-execute-listener
        ActionListener<ExecutePolicyResponse> listener = new ActionListener<>() {
            @Override
            public void onResponse(ExecutePolicyResponse response) { // <1>
                ExecutePolicyResponse.ExecutionStatus status =
                    response.getExecutionStatus();
            }

            @Override
            public void onFailure(Exception e) {
                // <2>
            }
        };
        // end::enrich-execute-policy-execute-listener

        // Replace the empty listener by a blocking listener in test
        final CountDownLatch latch = new CountDownLatch(1);
        listener = new LatchedActionListener<>(listener, latch);

        // tag::enrich-execute-policy-execute-async
        client.enrich().executePolicyAsync(request, RequestOptions.DEFAULT,
            listener); // <1>
        // end::enrich-execute-policy-execute-async

        assertTrue(latch.await(30L, TimeUnit.SECONDS));
    }

}
