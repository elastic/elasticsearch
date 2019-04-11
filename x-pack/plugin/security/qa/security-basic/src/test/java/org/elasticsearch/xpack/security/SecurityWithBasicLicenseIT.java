/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.yaml.ObjectPath;
import org.elasticsearch.xpack.security.authc.InternalRealms;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class SecurityWithBasicLicenseIT extends ESRestTestCase {

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

    public void testWithBasicLicense() throws Exception {
        checkLicenseType("basic");
        checkSecurityEnabled(false);
        checkAuthentication();
        checkHasPrivileges();
        checkIndexWrite();
    }

    public void testWithTrialLicense() throws Exception {
        startTrial();
        try {
            checkLicenseType("trial");
            checkSecurityEnabled(true);
            checkAuthentication();
            checkHasPrivileges();
            checkIndexWrite();
        } finally {
            revertTrial();
        }
    }

    private void startTrial() throws IOException {
        Response response = client().performRequest(new Request("POST", "/_license/start_trial?acknowledge=true"));
        assertOK(response);
    }

    private void revertTrial() throws IOException {
        client().performRequest(new Request("POST", "/_license/start_basic?acknowledge=true"));
    }

    private void checkLicenseType(String type) throws IOException {
        Map<String, Object> license = getAsMap("/_license");
        assertThat(license, notNullValue());
        assertThat(ObjectPath.evaluate(license, "license.type"), equalTo(type));
    }

    private void checkSecurityEnabled(boolean allowAllRealms) throws IOException {
        Map<String, Object> usage = getAsMap("/_xpack/usage");
        assertThat(usage, notNullValue());
        assertThat(ObjectPath.evaluate(usage, "security.available"), equalTo(true));
        assertThat(ObjectPath.evaluate(usage, "security.enabled"), equalTo(true));
        for (String realm : Arrays.asList("file", "native")) {
            assertThat(ObjectPath.evaluate(usage, "security.realms." + realm + ".available"), equalTo(true));
            assertThat(ObjectPath.evaluate(usage, "security.realms." + realm + ".enabled"), equalTo(true));
        }
        for (String realm : InternalRealms.getConfigurableRealmsTypes()) {
            if (realm.equals("file") == false && realm.equals("native") == false) {
                assertThat(ObjectPath.evaluate(usage, "security.realms." + realm + ".available"), equalTo(allowAllRealms));
                assertThat(ObjectPath.evaluate(usage, "security.realms." + realm + ".enabled"), equalTo(false));
            }
        }
    }

    private void checkAuthentication() throws IOException {
        final Map<String, Object> auth = getAsMap("/_security/_authenticate");
        // From file realm, configured in build.gradle
        assertThat(ObjectPath.evaluate(auth, "username"), equalTo("security_test_user"));
        assertThat(ObjectPath.evaluate(auth, "roles"), contains("security_test_role"));
    }

    private void checkHasPrivileges() throws IOException {
        final Request request = new Request("GET", "/_security/user/_has_privileges");
        request.setJsonEntity("{" +
            "\"cluster\": [ \"manage\", \"monitor\" ]," +
            "\"index\": [{ \"names\": [ \"index_allowed\", \"index_denied\" ], \"privileges\": [ \"read\", \"all\" ] }]" +
            "}");
        Response response = client().performRequest(request);
        final Map<String, Object> auth = entityAsMap(response);
        assertThat(ObjectPath.evaluate(auth, "username"), equalTo("security_test_user"));
        assertThat(ObjectPath.evaluate(auth, "has_all_requested"), equalTo(false));
        assertThat(ObjectPath.evaluate(auth, "cluster.manage"), equalTo(false));
        assertThat(ObjectPath.evaluate(auth, "cluster.monitor"), equalTo(true));
        assertThat(ObjectPath.evaluate(auth, "index.index_allowed.read"), equalTo(true));
        assertThat(ObjectPath.evaluate(auth, "index.index_allowed.all"), equalTo(false));
        assertThat(ObjectPath.evaluate(auth, "index.index_denied.read"), equalTo(false));
        assertThat(ObjectPath.evaluate(auth, "index.index_denied.all"), equalTo(false));
    }

    private void checkIndexWrite() throws IOException {
        final Request request1 = new Request("POST", "/index_allowed/_doc");
        request1.setJsonEntity("{ \"key\" : \"value\" }");
        Response response1 = client().performRequest(request1);
        final Map<String, Object> result1 = entityAsMap(response1);
        assertThat(ObjectPath.evaluate(result1, "_index"), equalTo("index_allowed"));
        assertThat(ObjectPath.evaluate(result1, "result"), equalTo("created"));

        final Request request2 = new Request("POST", "/index_denied/_doc");
        request2.setJsonEntity("{ \"key\" : \"value\" }");
        ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(request2));
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(403));
        assertThat(e.getMessage(), containsString("unauthorized for user [security_test_user]"));
    }

}
