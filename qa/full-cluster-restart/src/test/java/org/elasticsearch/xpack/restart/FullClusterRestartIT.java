/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.restart;

import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.Version;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.NotEqualMessageBuilder;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.ml.MachineLearningTemplateRegistry;
import org.elasticsearch.xpack.security.SecurityClusterClientYamlTestCase;
import org.elasticsearch.xpack.test.rest.XPackRestTestCase;
import org.junit.Before;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.containsString;

public class FullClusterRestartIT extends ESRestTestCase {
    private final boolean runningAgainstOldCluster = Booleans.parseBoolean(System.getProperty("tests.is_old_cluster"));
    private final Version oldClusterVersion = Version.fromString(System.getProperty("tests.old_cluster_version"));

    @Before
    public void waitForSecuritySetup() throws Exception {
        SecurityClusterClientYamlTestCase.waitForSecurity();
    }

    @Before
    public void waitForMlTemplates() throws Exception {
        XPackRestTestCase.waitForMlTemplates();
    }

    @Override
    protected boolean preserveIndicesUponCompletion() {
        return true;
    }

    @Override
    protected boolean preserveTemplatesUponCompletion() {
        return true;
    }

    @Override
    protected Settings restClientSettings() {
        String token = "Basic " + Base64.getEncoder().encodeToString("elastic:changeme".getBytes(StandardCharsets.UTF_8));
        return Settings.builder()
                .put(ThreadContext.PREFIX + ".Authorization", token)
                // we increase the timeout here to 90 seconds to handle long waits for a green
                // cluster health. the waits for green need to be longer than a minute to
                // account for delayed shards
                .put(ESRestTestCase.CLIENT_RETRY_TIMEOUT, "90s")
                .put(ESRestTestCase.CLIENT_SOCKET_TIMEOUT, "90s")
                .build();
    }

    /**
     * Tests that a single document survives. Super basic smoke test.
     */
    public void testSingleDoc() throws IOException {
        String docLocation = "/testsingledoc/doc/1";
        String doc = "{\"test\": \"test\"}";

        if (runningAgainstOldCluster) {
            client().performRequest("PUT", docLocation, singletonMap("refresh", "true"),
                    new StringEntity(doc, ContentType.APPLICATION_JSON));
        }

        assertThat(toStr(client().performRequest("GET", docLocation)), containsString(doc));
    }

    // This will only work when the upgrade API is in place!
    @AwaitsFix(bugUrl = "https://github.com/elastic/dev/issues/741")
    public void testSecurityNativeRealm() throws IOException {
        XContentBuilder userBuilder = JsonXContent.contentBuilder().startObject();
        userBuilder.field("password", "j@rV1s");
        userBuilder.array("roles", "admin", "other_role1");
        userBuilder.field("full_name", "Jack Nicholson");
        userBuilder.field("email", "jacknich@example.com");
        userBuilder.startObject("metadata"); {
            userBuilder.field("intelligence", 7);
        }
        userBuilder.endObject();
        userBuilder.field("enabled", true);
        String user = userBuilder.endObject().string();

        if (runningAgainstOldCluster) {
            client().performRequest("PUT", "/_xpack/security/user/jacknich", emptyMap(),
                    new StringEntity(user, ContentType.APPLICATION_JSON));
        }

        Map<String, Object> response = toMap(client().performRequest("GET", "/_xpack/security/user/jacknich"));
        Map<String, Object> expected = toMap(user);
        expected.put("username", "jacknich");
        expected.remove("password");
        expected = singletonMap("jacknich", expected);
        if (false == response.equals(expected)) {
            NotEqualMessageBuilder message = new NotEqualMessageBuilder();
            message.compareMaps(response, expected);
            fail("User doesn't match.\n" + message.toString());
        }
    }

    static Map<String, Object> toMap(Response response) throws IOException {
        return toMap(EntityUtils.toString(response.getEntity()));
    }

    static Map<String, Object> toMap(String response) throws IOException {
        return XContentHelper.convertToMap(JsonXContent.jsonXContent, response, false);
    }

    static String toStr(Response response) throws IOException {
        return EntityUtils.toString(response.getEntity());
    }
}
