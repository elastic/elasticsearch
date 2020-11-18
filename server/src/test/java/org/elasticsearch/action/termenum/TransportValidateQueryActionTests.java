/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.termenum;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.equalTo;

public class TransportValidateQueryActionTests extends ESSingleNodeTestCase {

    /*
     * Copy of test that tripped up similarly broadcast ValidateQuery
     */
    public void testListenerOnlyInvokedOnceWhenIndexDoesNotExist() {
        final AtomicBoolean invoked = new AtomicBoolean();
        final ActionListener<TermEnumResponse> listener = new ActionListener<>() {

            @Override
            public void onResponse(final TermEnumResponse validateQueryResponse) {
                fail("onResponse should not be invoked in this failure case");
            }

            @Override
            public void onFailure(final Exception e) {
                if (invoked.compareAndSet(false, true) == false) {
                    fail("onFailure invoked more than once");
                }
            }

        };
        client().admin().indices().termEnum(new TermEnumRequest("non-existent-index"), listener);
        assertThat(invoked.get(), equalTo(true)); // ensure that onFailure was invoked
    }

}
