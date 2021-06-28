/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.dlsfls;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.SecuritySettingsSourceField;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.security.SecurityOnTrialLicenseRestTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;

public class DlsRequestCacheIT extends SecurityOnTrialLicenseRestTestCase {

    private static final String DLS_USER = "system_user";
    private static final SecureString DLS_USER_PASSWORD = new SecureString("dls-user-password".toCharArray());
    private static final String DLS_TEMPLATE_PAINLESS_INDEX = "dls-template-painless-index";

    @Before
    public void createUsers() throws IOException {
        createUser(DLS_USER, DLS_USER_PASSWORD, List.of("dls_painless_role"));
    }

    @After
    public void cleanUp() throws IOException {
        deleteUser(DLS_USER);
    }

    @Override
    protected Settings restAdminSettings() {
        String token = basicAuthHeaderValue("x_pack_rest_user", SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING);
        return Settings.builder()
            .put(ThreadContext.PREFIX + ".Authorization", token)
            .build();
    }

    public void testRequestCacheDisabledForDlsTemplateRoleWithPainless() throws IOException {
        final RestClient adminClient = adminClient();
        final RestClient client = client();

        final Request putScriptRequest = new Request("PUT", "_scripts/range-now");
        putScriptRequest.setJsonEntity("{\"script\":{\"lang\":\"painless\"," +
            "\"source\":\"'{\\\"range\\\":{\\\"date\\\": {\\\"lte\\\": \\\"' + new Date().getTime() + '\\\"}}}' \"}}");
        assertOK(adminClient.performRequest(putScriptRequest));

        // Create the index with a date field and 1 primary shard with no replica
        final Request putIndexRequest = new Request("PUT", DLS_TEMPLATE_PAINLESS_INDEX);
        putIndexRequest.setJsonEntity("{\"mappings\":{\"properties\":{\"date\":{\"type\":\"date\",\"format\":\"epoch_millis\"}}}," +
            "\"settings\":{\"number_of_shards\":1,\"number_of_replicas\":0}}");
        assertOK(adminClient.performRequest(putIndexRequest));

        // A doc in the past 1 min
        final Request putDocRequest1 = new Request("PUT", DLS_TEMPLATE_PAINLESS_INDEX + "/_doc/1");
        putDocRequest1.setJsonEntity("{\"date\":" + Instant.now().minus(Duration.ofSeconds(60)).toEpochMilli() + "}");
        assertOK(adminClient.performRequest(putDocRequest1));

        // A doc in the future 1 min
        final Request putDocRequest2 = new Request("PUT", DLS_TEMPLATE_PAINLESS_INDEX + "/_doc/2");
        putDocRequest2.setJsonEntity("{\"date\":" + Instant.now().plus(Duration.ofSeconds(60)).toEpochMilli() + "}");
        assertOK(adminClient.performRequest(putDocRequest2));

        final Request refreshRequest = new Request("POST", DLS_TEMPLATE_PAINLESS_INDEX + "/_refresh");
        assertOK(adminClient.performRequest(refreshRequest));

        // First search should only get 1 doc in the past
        final Request searchRequest = new Request("GET", DLS_TEMPLATE_PAINLESS_INDEX + "/_search");
        searchRequest.addParameter("request_cache", "true");
        searchRequest.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization",
            UsernamePasswordToken.basicAuthHeaderValue(DLS_USER, DLS_USER_PASSWORD)));
        assertSearchResponse(client.performRequest(searchRequest), Set.of("1"));
        // Cache should not be used since DLS query uses stored script
        assertCacheState(0, 0);
    }

    @SuppressWarnings("unchecked")
    private void assertSearchResponse(Response response, Set<String> docIds) throws IOException {
        final Map<String, Object> m = responseAsMap(response);

        final Map<String, Object> hits = (Map<String, Object>) m.get("hits");
        final List<Map<String, Object>> docs = (List<Map<String, Object>>) hits.get("hits");

        assertThat(docs.stream().map(d -> (String) d.get("_id")).collect(Collectors.toSet()), equalTo(docIds));
    }

    @SuppressWarnings("unchecked")
    public void assertCacheState(int expectedHits, int expectedMisses) throws IOException {
        final Request request = new Request("GET", DLS_TEMPLATE_PAINLESS_INDEX + "/_stats");
        request.addParameter("filter_path", "indices." + DLS_TEMPLATE_PAINLESS_INDEX + ".total.request_cache");
        final Response response = adminClient().performRequest(request);
        final Map<String, Object> m = responseAsMap(response);
        final Map<String, Object> indices = (Map<String, Object>) m.get("indices");
        final Map<String, Object> index = (Map<String, Object>) indices.get(DLS_TEMPLATE_PAINLESS_INDEX);
        final Map<String, Object> total = (Map<String, Object>) index.get("total");
        final Map<String, Object> requestCache = (Map<String, Object>) total.get("request_cache");
        assertThat((int) requestCache.get("hit_count"), equalTo(expectedHits));
        assertThat((int) requestCache.get("miss_count"), equalTo(expectedMisses));
    }
}
