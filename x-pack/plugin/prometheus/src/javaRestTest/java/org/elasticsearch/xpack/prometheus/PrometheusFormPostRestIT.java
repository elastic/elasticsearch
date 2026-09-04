/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.prometheus;

import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.junit.Before;
import org.junit.ClassRule;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * Integration tests that form-encoded {@code POST} on Prometheus read routes fails when
 * {@code xpack.security.http.ssl.enabled} is {@code false}.
 *
 * <p>The cluster runs with HTTP SSL disabled. Each test sends an authenticated form-encoded
 * {@code POST} and expects {@code 406 Not Acceptable}, confirming that form bodies are rejected
 * unless HTTP SSL is enabled on the Elasticsearch HTTP interface.
 */
public class PrometheusFormPostRestIT extends ESRestTestCase {

    private static final String USER = "test_admin";
    private static final String PASS = "x-pack-test-password";

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .setting("xpack.ml.enabled", "false")
        .setting("xpack.watcher.enabled", "false")
        .setting("xpack.license.self_generated.type", "trial")
        .setting("xpack.security.enabled", "true")
        .setting("xpack.security.autoconfiguration.enabled", "false")
        .setting("xpack.security.transport.ssl.enabled", "false")
        .setting("xpack.security.authc.api_key.enabled", "true")
        .setting("xpack.security.http.ssl.enabled", "false")
        .user(USER, PASS, "superuser", false)
        .build();

    private String readApiKey;

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue(USER, new SecureString(PASS.toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    @Before
    public void createReadApiKey() throws IOException {
        readApiKey = createApiKey("prometheus-read-key", "metrics-*", "read");
    }

    public void testQueryFormPostRejectedWhenHttpSslDisabled() throws Exception {
        assertFormPostRejected(
            "/_prometheus/api/v1/query",
            new BasicNameValuePair("query", "up"),
            new BasicNameValuePair("time", "2026-01-01T00:05:00Z")
        );
    }

    public void testQueryRangeFormPostRejectedWhenHttpSslDisabled() throws Exception {
        assertFormPostRejected(
            "/_prometheus/api/v1/query_range",
            new BasicNameValuePair("query", "up"),
            new BasicNameValuePair("start", "2026-01-01T00:00:00Z"),
            new BasicNameValuePair("end", "2026-01-01T00:05:00Z"),
            new BasicNameValuePair("step", "60s")
        );
    }

    public void testSeriesFormPostRejectedWhenHttpSslDisabled() throws Exception {
        assertFormPostRejected("/_prometheus/api/v1/series", new BasicNameValuePair("match[]", "up"));
    }

    public void testLabelsFormPostRejectedWhenHttpSslDisabled() throws Exception {
        assertFormPostRejected("/_prometheus/api/v1/labels", new BasicNameValuePair("match[]", "up"));
    }

    /**
     * Asserts that an authenticated form-encoded {@code POST} to {@code path} is rejected when HTTP SSL is not enabled.
     */
    private void assertFormPostRejected(String path, NameValuePair... params) throws Exception {
        Request request = new Request("POST", path);
        request.setEntity(new UrlEncodedFormEntity(Arrays.asList(params), StandardCharsets.UTF_8));
        request.setOptions(request.getOptions().toBuilder().addHeader("Authorization", "ApiKey " + readApiKey).build());

        ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(request));
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(406));
        assertThat(EntityUtils.toString(e.getResponse().getEntity()), containsString("application/x-www-form-urlencoded"));
    }

    private String createApiKey(String name, String indexPattern, String... privileges) throws IOException {
        StringBuilder privilegeArray = new StringBuilder();
        for (int i = 0; i < privileges.length; i++) {
            if (i > 0) {
                privilegeArray.append("\", \"");
            }
            privilegeArray.append(privileges[i]);
        }
        Request request = new Request("POST", "/_security/api_key");
        request.setJsonEntity("""
            {
              "name": "$NAME",
              "role_descriptors": {
                "role": {
                  "index": [
                    {
                      "names": ["$INDEX_PATTERN"],
                      "privileges": ["$PRIVILEGES"]
                    }
                  ]
                }
              }
            }
            """.replace("$NAME", name).replace("$INDEX_PATTERN", indexPattern).replace("$PRIVILEGES", privilegeArray));
        ObjectPath response = ObjectPath.createFromResponse(client().performRequest(request));
        return response.evaluate("encoded");
    }
}
