/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.fleet;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.SecuritySettingsSourceField;
import org.elasticsearch.xcontent.XContentType;

import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class FleetSystemIndicesIT extends AbstractFleetIT {

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

    public void testCreationOfFleetFiles() throws Exception {
        Request request = new Request("POST", ".fleet-fileds-fromhost-meta-agent/_doc");
        request.setJsonEntity("{\"@timestamp\": 0}");

        Response response = client().performRequest(request);
        assertEquals(201, response.getStatusLine().getStatusCode());

        request = new Request("GET", ".fleet-fileds-fromhost-meta-agent/_mapping");
        response = client().performRequest(request);
        String responseBody = EntityUtils.toString(response.getEntity());
        assertThat(responseBody, not(containsString("xpack.fleet.template.version"))); // assert templating worked
        assertThat(responseBody, containsString("action_id"));
    }

    public void testCreationOfFleetFileData() throws Exception {
        Request request = new Request("POST", ".fleet-fileds-fromhost-data-agent/_doc");
        request.setJsonEntity("{\"@timestamp\": 0}");
        Response response = client().performRequest(request);
        assertEquals(201, response.getStatusLine().getStatusCode());

        request = new Request("GET", ".fleet-fileds-fromhost-data-agent/_mapping");
        response = client().performRequest(request);
        String responseBody = EntityUtils.toString(response.getEntity());
        assertThat(responseBody, not(containsString("xpack.fleet.template.version"))); // assert templating worked
        assertThat(responseBody, containsString("data"));
        assertThat(responseBody, containsString("bid"));
    }

    public void testCreationOfFleetFileDelivery() throws Exception {
        Request request = new Request("POST", ".fleet-fileds-tohost-meta-agent/_doc");
        request.setJsonEntity("{\"@timestamp\": 0}");
        Response response = client().performRequest(request);
        assertEquals(201, response.getStatusLine().getStatusCode());

        request = new Request("GET", ".fleet-fileds-tohost-meta-agent/_mapping");
        response = client().performRequest(request);
        String responseBody = EntityUtils.toString(response.getEntity());
        assertThat(responseBody, not(containsString("xpack.fleet.template.version"))); // assert templating worked
        assertThat(responseBody, containsString("action_id"));
    }

    public void testCreationOfFleetFileDeliveryData() throws Exception {
        Request request = new Request("POST", ".fleet-fileds-tohost-data-agent/_doc");
        request.setJsonEntity("{\"@timestamp\": 0}");
        Response response = client().performRequest(request);
        assertEquals(201, response.getStatusLine().getStatusCode());

        request = new Request("GET", ".fleet-fileds-tohost-data-agent/_mapping");
        response = client().performRequest(request);
        String responseBody = EntityUtils.toString(response.getEntity());
        assertThat(responseBody, not(containsString("xpack.fleet.template.version"))); // assert templating worked
        assertThat(responseBody, containsString("data"));
        assertThat(responseBody, containsString("bid"));
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
    public void verifyActionsILMPolicyExists() throws Exception {
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

    @SuppressWarnings("unchecked")
    public void verifyFilesILMPolicyExists() throws Exception {
        assertBusy(() -> {
            Request request = new Request("GET", "_ilm/policy/.fleet-file-fromhost-meta-ilm-policy");
            Response response = client().performRequest(request);
            assertEquals(200, response.getStatusLine().getStatusCode());
            final String responseJson = EntityUtils.toString(response.getEntity());
            Map<String, Object> responseMap = XContentHelper.convertToMap(XContentType.JSON.xContent(), responseJson, false);
            assertNotNull(responseMap.get(".fleet-file-fromhost-meta-ilm-policy"));
            Map<String, Object> policyMap = (Map<String, Object>) responseMap.get(".fleet-file-fromhost-meta-ilm-policy");
            assertNotNull(policyMap);
            assertThat(policyMap.size(), equalTo(2));
        });
    }

    @SuppressWarnings("unchecked")
    public void verifyFileDataILMPolicyExists() throws Exception {
        assertBusy(() -> {
            Request request = new Request("GET", "_ilm/policy/.fleet-file-fromhost-data-ilm-policy");
            Response response = client().performRequest(request);
            assertEquals(200, response.getStatusLine().getStatusCode());
            final String responseJson = EntityUtils.toString(response.getEntity());
            Map<String, Object> responseMap = XContentHelper.convertToMap(XContentType.JSON.xContent(), responseJson, false);
            assertNotNull(responseMap.get(".fleet-file-fromhost-data-ilm-policy"));
            Map<String, Object> policyMap = (Map<String, Object>) responseMap.get(".fleet-file-fromhost-data-ilm-policy");
            assertNotNull(policyMap);
            assertThat(policyMap.size(), equalTo(2));
        });
    }

    @SuppressWarnings("unchecked")
    public void verifyFileDeliveryILMPolicyExists() throws Exception {
        assertBusy(() -> {
            Request request = new Request("GET", "_ilm/policy/.fleet-file-tohost-meta-ilm-policy");
            Response response = client().performRequest(request);
            assertEquals(200, response.getStatusLine().getStatusCode());
            final String responseJson = EntityUtils.toString(response.getEntity());
            Map<String, Object> responseMap = XContentHelper.convertToMap(XContentType.JSON.xContent(), responseJson, false);
            assertNotNull(responseMap.get(".fleet-file-tohost-meta-ilm-policy"));
            Map<String, Object> policyMap = (Map<String, Object>) responseMap.get(".fleet-file-tohost-meta-ilm-policy");
            assertNotNull(policyMap);
            assertThat(policyMap.size(), equalTo(2));
        });
    }

    @SuppressWarnings("unchecked")
    public void verifyFileDeliveryDataILMPolicyExists() throws Exception {
        assertBusy(() -> {
            Request request = new Request("GET", "_ilm/policy/.fleet-file-tohost-data-ilm-policy");
            Response response = client().performRequest(request);
            assertEquals(200, response.getStatusLine().getStatusCode());
            final String responseJson = EntityUtils.toString(response.getEntity());
            Map<String, Object> responseMap = XContentHelper.convertToMap(XContentType.JSON.xContent(), responseJson, false);
            assertNotNull(responseMap.get(".fleet-file-tohost-data-ilm-policy"));
            Map<String, Object> policyMap = (Map<String, Object>) responseMap.get(".fleet-file-tohost-data-ilm-policy");
            assertNotNull(policyMap);
            assertThat(policyMap.size(), equalTo(2));
        });
    }
}
