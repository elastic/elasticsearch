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
package org.elasticsearch.client;

import org.elasticsearch.client.core.AcknowledgedResponse;
import org.elasticsearch.client.enrich.GetPolicyRequest;
import org.elasticsearch.client.enrich.GetPolicyResponse;
import org.elasticsearch.client.enrich.PutPolicyRequest;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class EnrichIT extends ESRestHighLevelClientTestCase {

    public void testCRUD() throws Exception {
        final EnrichClient enrichClient = highLevelClient().enrich();
        PutPolicyRequest putPolicyRequest =
            new PutPolicyRequest("my-policy", "exact_match", List.of("my-index"), "enrich_key", List.of("enrich_value"));
        AcknowledgedResponse putPolicyResponse = execute(putPolicyRequest, enrichClient::putPolicy, enrichClient::putPolicyAsync);
        assertThat(putPolicyResponse.isAcknowledged(), is(true));

        GetPolicyRequest getPolicyRequest = new GetPolicyRequest("my-policy");
        GetPolicyResponse getPolicyResponse = execute(getPolicyRequest, enrichClient::getPolicy, enrichClient::getPolicyAsync);
        assertThat(getPolicyResponse.getPolicy(), notNullValue());
        assertThat(getPolicyResponse.getPolicy().getType(), equalTo(putPolicyRequest.getType()));
        assertThat(getPolicyResponse.getPolicy().getIndices(), equalTo(putPolicyRequest.getIndices()));
        assertThat(getPolicyResponse.getPolicy().getEnrichKey(), equalTo(putPolicyRequest.getEnrichKey()));
        assertThat(getPolicyResponse.getPolicy().getEnrichValues(), equalTo(putPolicyRequest.getEnrichValues()));
    }

}
