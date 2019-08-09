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
import org.elasticsearch.client.enrich.PutPolicyRequest;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class EnrichDocumentationIT extends ESRestHighLevelClientTestCase {

    public void testPutPolicy() throws Exception {
        RestHighLevelClient client = highLevelClient();
        // tag::enrich-put-policy-request
        PutPolicyRequest putPolicyRequest = new PutPolicyRequest(
            "users-policy", "exact_match", Arrays.asList("users"),
            "email", Arrays.asList("address", "zip", "city", "state"));
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
        ActionListener<AcknowledgedResponse> listener =
            new ActionListener<AcknowledgedResponse>() {
                @Override
                public void onResponse(AcknowledgedResponse response) { // <1>
                    boolean isAcknowledged =
                        putPolicyResponse.isAcknowledged();
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

}
