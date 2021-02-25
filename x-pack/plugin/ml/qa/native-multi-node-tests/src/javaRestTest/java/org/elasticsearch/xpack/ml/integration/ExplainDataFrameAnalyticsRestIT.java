/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.integration;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.SecuritySettingsSourceField;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;

public class ExplainDataFrameAnalyticsRestIT extends ESRestTestCase {

    private static String basicAuth(String user) {
        return UsernamePasswordToken.basicAuthHeaderValue(user, SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING);
    }

    private static final String SUPER_USER = "x_pack_rest_user";
    private static final String ML_ADMIN = "ml_admin";
    private static final String BASIC_AUTH_VALUE_SUPER_USER = basicAuth(SUPER_USER);
    private static final String AUTH_KEY = "Authorization";
    private static final String SECONDARY_AUTH_KEY = "es-secondary-authorization";

    private static RequestOptions.Builder addAuthHeader(RequestOptions.Builder builder, String user) {
        builder.addHeader(AUTH_KEY, basicAuth(user));
        return builder;
    }

    private static RequestOptions.Builder addSecondaryAuthHeader(RequestOptions.Builder builder, String user) {
        builder.addHeader(SECONDARY_AUTH_KEY, basicAuth(user));
        return builder;
    }

    @Override
    protected Settings restClientSettings() {
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", BASIC_AUTH_VALUE_SUPER_USER).build();
    }

    private void setupUser(String user, List<String> roles) throws IOException {
        String password = new String(SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING.getChars());

        Request request = new Request("PUT", "/_security/user/" + user);
        request.setJsonEntity("{"
                + "  \"password\" : \"" + password + "\","
                + "  \"roles\" : [ " + roles.stream().map(unquoted -> "\"" + unquoted + "\"").collect(Collectors.joining(", ")) + " ]"
                + "}");
        client().performRequest(request);
    }

    @Before
    public void setUpData() throws Exception {
        // This user has admin rights on machine learning, but (importantly for the tests) no rights
        // on any of the data indexes
        setupUser(ML_ADMIN, Collections.singletonList("machine_learning_admin"));
        addAirlineData();
    }

    private void addAirlineData() throws IOException {
        StringBuilder bulk = new StringBuilder();

        // Create index with source = enabled, doc_values = enabled, stored = false + multi-field
        Request createAirlineDataRequest = new Request("PUT", "/airline-data");
        createAirlineDataRequest.setJsonEntity("{"
                + "  \"mappings\": {"
                + "    \"properties\": {"
                + "      \"time stamp\": { \"type\":\"date\"}," // space in 'time stamp' is intentional
                + "      \"airline\": {"
                + "        \"type\":\"keyword\""
                + "       },"
                + "      \"responsetime\": { \"type\":\"float\"}"
            + "    }"
                + "  }"
                + "}");
        client().performRequest(createAirlineDataRequest);

        bulk.append("{\"index\": {\"_index\": \"airline-data\", \"_id\": 1}}\n");
        bulk.append("{\"time stamp\":\"2016-06-01T00:00:00Z\",\"airline\":\"AAA\",\"responsetime\":135.22}\n");
        bulk.append("{\"index\": {\"_index\": \"airline-data\", \"_id\": 2}}\n");
        bulk.append("{\"time stamp\":\"2016-06-01T01:59:00Z\",\"airline\":\"AAA\",\"responsetime\":541.76}\n");

        bulkIndex(bulk.toString());
    }

    public void testExplain_GivenSecondaryHeadersAndConfig() throws IOException {
        String config = "{\n" +
            "  \"source\": {\n" +
            "    \"index\": \"airline-data\"\n" +
            "  },\n" +
            "  \"analysis\": {\n" +
            "    \"regression\": {\n" +
            "      \"dependent_variable\": \"responsetime\"\n" +
            "    }\n" +
            "  }\n" +
            "}";


        { // Request with secondary headers without perms
            Request explain = explainRequestViaConfig(config);
            RequestOptions.Builder options = explain.getOptions().toBuilder();
            addAuthHeader(options, SUPER_USER);
            addSecondaryAuthHeader(options, ML_ADMIN);
            explain.setOptions(options);
            // Should throw
            expectThrows(ResponseException.class, () -> client().performRequest(explain));
        }
        { // request with secondary headers with perms
            Request explain = explainRequestViaConfig(config);
            RequestOptions.Builder options = explain.getOptions().toBuilder();
            addAuthHeader(options, ML_ADMIN);
            addSecondaryAuthHeader(options, SUPER_USER);
            explain.setOptions(options);
            // Should not throw
            client().performRequest(explain);
        }
    }

    public void testExplain_GivenSecondaryHeadersAndPreviouslyStoredConfig() throws IOException {
        String config = "{\n" +
            "  \"source\": {\n" +
            "    \"index\": \"airline-data\"\n" +
            "  },\n" +
            "  \"dest\": {\n" +
            "    \"index\": \"response_prediction\"\n" +
            "  },\n" +
            "  \"analysis\":\n" +
            "    {\n" +
            "      \"regression\": {\n" +
            "        \"dependent_variable\": \"responsetime\"\n" +
            "      }\n" +
            "    }\n" +
            "}";

        String configId = "explain_test";

        Request storeConfig = new Request("PUT", "_ml/data_frame/analytics/" + configId);
        storeConfig.setJsonEntity(config);
        client().performRequest(storeConfig);

        { // Request with secondary headers without perms
            Request explain = explainRequestConfigId(configId);
            RequestOptions.Builder options = explain.getOptions().toBuilder();
            addAuthHeader(options, SUPER_USER);
            addSecondaryAuthHeader(options, ML_ADMIN);
            explain.setOptions(options);
            // Should throw
            expectThrows(ResponseException.class, () -> client().performRequest(explain));
        }
        { // request with secondary headers with perms
            Request explain = explainRequestConfigId(configId);
            RequestOptions.Builder options = explain.getOptions().toBuilder();
            addAuthHeader(options, ML_ADMIN);
            addSecondaryAuthHeader(options, SUPER_USER);
            explain.setOptions(options);
            // Should not throw
            client().performRequest(explain);
        }
    }

    private static Request explainRequestViaConfig(String config) {
        Request request = new Request("POST", "_ml/data_frame/analytics/_explain");
        request.setJsonEntity(config);
        return request;
    }

    private static Request explainRequestConfigId(String id) {
        return new Request("POST", "_ml/data_frame/analytics/" + id + "/_explain");
    }

    private void bulkIndex(String bulk) throws IOException {
        Request bulkRequest = new Request("POST", "/_bulk");
        bulkRequest.setJsonEntity(bulk);
        bulkRequest.addParameter("refresh", "true");
        bulkRequest.addParameter("pretty", null);
        String bulkResponse = EntityUtils.toString(client().performRequest(bulkRequest).getEntity());
        assertThat(bulkResponse, not(containsString("\"errors\": false")));
    }

}
