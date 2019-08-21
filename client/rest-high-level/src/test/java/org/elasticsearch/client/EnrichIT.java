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

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.core.AcknowledgedResponse;
import org.elasticsearch.client.enrich.PutPolicyRequest;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class EnrichIT extends ESRestHighLevelClientTestCase {

    public void testCRUD() throws Exception {
        final EnrichClient enrichClient = highLevelClient().enrich();
        PutPolicyRequest putPolicyRequest = new PutPolicyRequest("my-policy", "exact_match",
            Collections.singletonList("my-index"), "enrich_key", Collections.singletonList("enrich_value"));
        AcknowledgedResponse putPolicyResponse = execute(putPolicyRequest, enrichClient::putPolicy, enrichClient::putPolicyAsync);
        assertThat(putPolicyResponse.isAcknowledged(), is(true));

        // TODO: Replace with get policy hlrc code:
        Request getPolicyRequest = new Request("get", "/_enrich/policy/my-policy");
        Response getPolicyResponse = highLevelClient().getLowLevelClient().performRequest(getPolicyRequest);
        assertThat(getPolicyResponse.getHttpResponse().getStatusLine().getStatusCode(), equalTo(200));
        Map<String, Object> responseBody = toMap(getPolicyResponse);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> responsePolicies = (List<Map<String, Object>>) responseBody.get("policies");
        assertThat(responsePolicies.size(), equalTo(1));
        assertThat(responsePolicies.get(0).get("type"), equalTo(putPolicyRequest.getType()));
        assertThat(responsePolicies.get(0).get("indices"), equalTo(putPolicyRequest.getIndices()));
        assertThat(responsePolicies.get(0).get("match_field"), equalTo(putPolicyRequest.getMatchField()));
        assertThat(responsePolicies.get(0).get("enrich_fields"), equalTo(putPolicyRequest.getEnrichFields()));
    }

    private static Map<String, Object> toMap(Response response) throws IOException {
        return XContentHelper.convertToMap(JsonXContent.jsonXContent, EntityUtils.toString(response.getEntity()), false);
    }

}
