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
import org.elasticsearch.client.security.PutUserRequest;
import org.elasticsearch.client.security.PutUserResponse;
import org.elasticsearch.client.security.RefreshPolicy;

import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class SecurityDocumentationIT extends ESRestHighLevelClientTestCase {

    public void testPutUser() throws Exception {
        RestHighLevelClient client = highLevelClient();

        {
            //tag::x-pack-put-user-execute
            char[] password = new char[] { 'p', 'a', 's', 's', 'w', 'o', 'r', 'd' };
            PutUserRequest request =
                new PutUserRequest("example", password, Collections.singletonList("superuser"), null, null, true, null, RefreshPolicy.NONE);
            PutUserResponse response = client.security().putUser(request, RequestOptions.DEFAULT);
            //end::x-pack-put-user-execute

            //tag::x-pack-put-user-response
            boolean isCreated = response.isCreated(); // <1>
            //end::x-pack-put-user-response

            assertTrue(isCreated);
        }

        {
            char[] password = new char[] { 'p', 'a', 's', 's', 'w', 'o', 'r', 'd' };
            PutUserRequest request = new PutUserRequest("example2", password, Collections.singletonList("superuser"), null, null, true,
                null, RefreshPolicy.NONE);
            // tag::x-pack-put-user-execute-listener
            ActionListener<PutUserResponse> listener = new ActionListener<PutUserResponse>() {
                @Override
                public void onResponse(PutUserResponse response) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            // end::x-pack-put-user-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::x-pack-put-user-execute-async
            client.security().putUserAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::x-pack-put-user-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }
}
