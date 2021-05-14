/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

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

package org.elasticsearch.xpack.fleet;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.SecuritySettingsSourceField;
import org.elasticsearch.test.rest.ESRestTestCase;

import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class FleetSystemIndicesIT extends ESRestTestCase {

    static final String BASIC_AUTH_VALUE = basicAuthHeaderValue(
        "x_pack_rest_user",
        SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING
    );

    @Override
    protected Settings restClientSettings() {
        return Settings.builder()
            .put(ThreadContext.PREFIX + ".Authorization", BASIC_AUTH_VALUE)
            .put(ThreadContext.PREFIX + ".X-elastic-product-origin", "fleet")
            .build();
    }

    public void testSearchWithoutIndexCreatedIsAllowed() throws Exception {
        Request request = new Request("GET", ".fleet-agents/_search");
        request.setJsonEntity("{ \"query\": { \"match_all\": {} } }");
        request.addParameter("ignore_unavailable", Boolean.TRUE.toString());

        assertEquals(200, client().performRequest(request).getStatusLine().getStatusCode());
    }

    public void testCreationOfFleetAgents() throws Exception {
        Request request = new Request("PUT", ".fleet-agents");
        Response response = client().performRequest(request);
        assertEquals(200, response.getStatusLine().getStatusCode());

        request = new Request("GET", ".fleet-agents/_mapping");
        response = client().performRequest(request);
        String responseBody = EntityUtils.toString(response.getEntity());
        assertThat(responseBody, containsString("access_api_key_id"));

        request = new Request("GET", ".fleet-agents-7/_mapping");
        response = client().performRequest(request);
        responseBody = EntityUtils.toString(response.getEntity());
        assertThat(responseBody, containsString("access_api_key_id"));
    }

    public void testCreationOfFleetActions() throws Exception {
        Request request = new Request("PUT", ".fleet-actions");
        Response response = client().performRequest(request);
        assertEquals(200, response.getStatusLine().getStatusCode());

        request = new Request("GET", ".fleet-actions/_mapping");
        response = client().performRequest(request);
        String responseBody = EntityUtils.toString(response.getEntity());
        assertThat(responseBody, containsString("action_id"));

        request = new Request("GET", ".fleet-actions-7/_mapping");
        response = client().performRequest(request);
        responseBody = EntityUtils.toString(response.getEntity());
        assertThat(responseBody, containsString("action_id"));
    }

    public void testCreationOfFleetArtifacts() throws Exception {
        Request request = new Request("PUT", ".fleet-artifacts");
        Response response = client().performRequest(request);
        assertEquals(200, response.getStatusLine().getStatusCode());

        request = new Request("GET", ".fleet-artifacts/_mapping");
        response = client().performRequest(request);
        String responseBody = EntityUtils.toString(response.getEntity());
        assertThat(responseBody, containsString("encryption_algorithm"));

        request = new Request("GET", ".fleet-artifacts-7/_mapping");
        response = client().performRequest(request);
        responseBody = EntityUtils.toString(response.getEntity());
        assertThat(responseBody, containsString("encryption_algorithm"));
    }

    public void testCreationOfFleetEnrollmentApiKeys() throws Exception {
        Request request = new Request("PUT", ".fleet-enrollment-api-keys");
        Response response = client().performRequest(request);
        assertEquals(200, response.getStatusLine().getStatusCode());

        request = new Request("GET", ".fleet-enrollment-api-keys/_mapping");
        response = client().performRequest(request);
        String responseBody = EntityUtils.toString(response.getEntity());
        assertThat(responseBody, containsString("api_key_id"));

        request = new Request("GET", ".fleet-enrollment-api-keys-7/_mapping");
        response = client().performRequest(request);
        responseBody = EntityUtils.toString(response.getEntity());
        assertThat(responseBody, containsString("api_key_id"));
    }

    public void testCreationOfFleetPolicies() throws Exception {
        Request request = new Request("PUT", ".fleet-policies");
        Response response = client().performRequest(request);
        assertEquals(200, response.getStatusLine().getStatusCode());

        request = new Request("GET", ".fleet-policies/_mapping");
        response = client().performRequest(request);
        String responseBody = EntityUtils.toString(response.getEntity());
        assertThat(responseBody, containsString("coordinator_idx"));

        request = new Request("GET", ".fleet-policies-7/_mapping");
        response = client().performRequest(request);
        responseBody = EntityUtils.toString(response.getEntity());
        assertThat(responseBody, containsString("coordinator_idx"));
    }

    public void testCreationOfFleetPoliciesLeader() throws Exception {
        Request request = new Request("PUT", ".fleet-policies-leader");
        Response response = client().performRequest(request);
        assertEquals(200, response.getStatusLine().getStatusCode());

        request = new Request("GET", ".fleet-policies-leader/_mapping");
        response = client().performRequest(request);
        String responseBody = EntityUtils.toString(response.getEntity());
        assertThat(responseBody, containsString("server"));

        request = new Request("GET", ".fleet-policies-leader-7/_mapping");
        response = client().performRequest(request);
        responseBody = EntityUtils.toString(response.getEntity());
        assertThat(responseBody, containsString("server"));
    }

    public void testCreationOfFleetServers() throws Exception {
        Request request = new Request("PUT", ".fleet-servers");
        Response response = client().performRequest(request);
        assertEquals(200, response.getStatusLine().getStatusCode());

        request = new Request("GET", ".fleet-servers/_mapping");
        response = client().performRequest(request);
        String responseBody = EntityUtils.toString(response.getEntity());
        assertThat(responseBody, containsString("architecture"));

        request = new Request("GET", ".fleet-servers-7/_mapping");
        response = client().performRequest(request);
        responseBody = EntityUtils.toString(response.getEntity());
        assertThat(responseBody, containsString("architecture"));
    }

    public void testCreationOfFleetActionsResults() throws Exception {
        Request request = new Request("POST", "/.fleet-actions-results/_doc");
        request.setJsonEntity("{ \"@timestamp\": \"2099-03-08T11:06:07.000Z\", \"agent_id\": \"my-agent\" }");
        Response response = client().performRequest(request);
        assertEquals(201, response.getStatusLine().getStatusCode());
    }

    @SuppressWarnings("unchecked")
    public void verifyILMPolicyExists() throws Exception {
        assertBusy(() -> {
            Request request = new Request("GET", "_ilm/policy/.fleet-actions-results-ilm-policy");
            Response response = client().performRequest(request);
            assertEquals(200, response.getStatusLine().getStatusCode());
            final String responseJson = EntityUtils.toString(response.getEntity());
            Map<String, Object> responseMap = XContentHelper.convertToMap(XContentType.JSON.xContent(), responseJson, false);
            assertNotNull(responseMap.get(".fleet-actions-results-ilm-policy"));
            Map<String, Object> policyMap = (Map<String, Object>) responseMap.get(".fleet-actions-results-ilm-policy");
            assertNotNull(policyMap);
            assertThat(policyMap.size(), equalTo(2));
        });
    }
}
