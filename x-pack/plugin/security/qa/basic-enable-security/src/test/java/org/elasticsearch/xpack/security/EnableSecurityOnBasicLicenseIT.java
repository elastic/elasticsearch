/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.yaml.ObjectPath;
import org.elasticsearch.xpack.security.authc.InternalRealms;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.Arrays;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class EnableSecurityOnBasicLicenseIT extends ESRestTestCase {

    private static boolean securityEnabled;

    @BeforeClass
    public static void checkTestMode() {
        final String hasSecurity = System.getProperty("tests.has_security");
        securityEnabled = Booleans.parseBoolean(hasSecurity);
    }

    @Override
    protected Settings restAdminSettings() {
        String token = basicAuthHeaderValue("admin_user", new SecureString("admin-password".toCharArray()));
        return Settings.builder()
            .put(ThreadContext.PREFIX + ".Authorization", token)
            .build();
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue("security_test_user", new SecureString("security-test-password".toCharArray()));
        return Settings.builder()
            .put(ThreadContext.PREFIX + ".Authorization", token)
            .build();
    }

    @Override
    protected boolean preserveClusterUponCompletion() {
        // If this is the first run (security not yet enabled), then don't clean up afterwards because we want to test restart with data
        return securityEnabled == false;
    }

    public void testSecuritySetup() throws Exception {
        logger.info("Security status: {}", securityEnabled);
        logger.info("Cluster:\n{}", getClusterInfo());
        logger.info("Indices:\n{}", getIndices());
        checkBasicLicenseType();

        checkSecurityStatus(securityEnabled);
        if (securityEnabled) {
            checkAuthentication();
        }

        checkAllowedWrite("index_allowed");
        // Security runs second, and should see the doc from the first (non-security) run
        final int expectedIndexCount = securityEnabled ? 2 : 1;
        checkIndexCount("index_allowed", expectedIndexCount);

        final String otherIndex = "index_" + randomAlphaOfLengthBetween(2, 6).toLowerCase(Locale.ROOT);
        if (securityEnabled) {
            checkDeniedWrite(otherIndex);
        } else {
            checkAllowedWrite(otherIndex);
        }
    }

    private String getClusterInfo() throws IOException {
        Map<String, Object> info = getAsMap("/");
        assertThat(info, notNullValue());
        return info.toString();
    }

    private String getIndices() throws IOException {
        final Request request = new Request("GET", "/_cat/indices");
        Response response = client().performRequest(request);
        return EntityUtils.toString(response.getEntity());
    }

    private void checkBasicLicenseType() throws IOException {
        Map<String, Object> license = getAsMap("/_license");
        assertThat(license, notNullValue());
        assertThat(ObjectPath.evaluate(license, "license.type"), equalTo("basic"));
    }

    private void checkSecurityStatus(boolean expectEnabled) throws IOException {
        Map<String, Object> usage = getAsMap("/_xpack/usage");
        assertThat(usage, notNullValue());
        assertThat(ObjectPath.evaluate(usage, "security.available"), equalTo(true));
        assertThat(ObjectPath.evaluate(usage, "security.enabled"), equalTo(expectEnabled));
        if (expectEnabled) {
            for (String realm : Arrays.asList("file", "native")) {
                assertThat(ObjectPath.evaluate(usage, "security.realms." + realm + ".available"), equalTo(true));
                assertThat(ObjectPath.evaluate(usage, "security.realms." + realm + ".enabled"), equalTo(true));
            }
            for (String realm : InternalRealms.getConfigurableRealmsTypes()) {
                if (realm.equals("file") == false && realm.equals("native") == false) {
                    assertThat(ObjectPath.evaluate(usage, "security.realms." + realm + ".available"), equalTo(false));
                    assertThat(ObjectPath.evaluate(usage, "security.realms." + realm + ".enabled"), equalTo(false));
                }
            }
        }
    }

    private void checkAuthentication() throws IOException {
        final Map<String, Object> auth = getAsMap("/_security/_authenticate");
        // From file realm, configured in build.gradle
        assertThat(ObjectPath.evaluate(auth, "username"), equalTo("security_test_user"));
        assertThat(ObjectPath.evaluate(auth, "roles"), containsInAnyOrder("security_test_role", "anonymous"));
    }

    private void checkAllowedWrite(String indexName) throws IOException {
        final Request request = new Request("POST", "/" + indexName + "/_doc");
        request.setJsonEntity("{ \"key\" : \"value\" }");
        Response response = client().performRequest(request);
        final Map<String, Object> result = entityAsMap(response);
        assertThat(ObjectPath.evaluate(result, "_index"), equalTo(indexName));
        assertThat(ObjectPath.evaluate(result, "result"), equalTo("created"));
    }

    private void checkDeniedWrite(String indexName) {
        final Request request = new Request("POST", "/" + indexName + "/_doc");
        request.setJsonEntity("{ \"key\" : \"value\" }");
        ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(request));
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(403));
        assertThat(e.getMessage(), containsString("unauthorized for user [security_test_user]"));
    }

    private void checkIndexCount(String indexName, int expectedCount) throws IOException {
        final Request request = new Request("POST", "/" + indexName + "/_refresh");
        adminClient().performRequest(request);

        final Map<String, Object> result = getAsMap("/" + indexName + "/_count");
        assertThat(ObjectPath.evaluate(result, "count"), equalTo(expectedCount));
    }
}
