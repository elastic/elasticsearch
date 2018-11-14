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
import org.elasticsearch.client.ESRestHighLevelClientTestCase;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.AcknowledgedResponse;
import org.elasticsearch.client.indexlifecycle.DeleteAction;
import org.elasticsearch.client.indexlifecycle.DeleteLifecyclePolicyRequest;
import org.elasticsearch.client.indexlifecycle.LifecycleAction;
import org.elasticsearch.client.indexlifecycle.LifecyclePolicy;
import org.elasticsearch.client.indexlifecycle.Phase;
import org.elasticsearch.client.indexlifecycle.PutLifecyclePolicyRequest;
import org.elasticsearch.client.indexlifecycle.RolloverAction;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

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

    static Map<String, Object> toMap(Response response) throws IOException {
        return XContentHelper.convertToMap(JsonXContent.jsonXContent, EntityUtils.toString(response.getEntity()), false);
    }

}
