/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security;

import com.carrotsearch.randomizedtesting.annotations.TestCaseOrdering;

import org.apache.http.HttpHost;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.Version;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.AnnotationTestOrdering;
import org.elasticsearch.test.AnnotationTestOrdering.Order;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.MutableSettingsProvider;
import org.elasticsearch.test.cluster.SettingsProvider;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.yaml.ObjectPath;
import org.elasticsearch.xpack.security.authc.InternalRealms;
import org.hamcrest.Matchers;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

@TestCaseOrdering(AnnotationTestOrdering.class)
public class EnableSecurityOnBasicLicenseIT extends ESRestTestCase {
    private static Boolean securityExplicitlySet = null;

    private static MutableSettingsProvider clusterSettings = new MutableSettingsProvider() {
        {
            put("xpack.ml.enabled", "false");
            put("xpack.license.self_generated.type", "basic");
        }
    };

    private static SettingsProvider explicitSecurity = s -> {
        // We have to initialize this field this way since we cannot access randomized runner context in static initializers
        if (securityExplicitlySet == null) {
            securityExplicitlySet = randomBoolean();
        }

        return securityExplicitlySet ? Collections.singletonMap("xpack.security.enabled", "false") : Collections.emptyMap();
    };

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .nodes(2)
        .distribution(DistributionType.DEFAULT)
        .settings(explicitSecurity)
        .settings(clusterSettings)
        .configFile("transport.key", Resource.fromClasspath("ssl/transport.key"))
        .configFile("transport.crt", Resource.fromClasspath("ssl/transport.crt"))
        .configFile("ca.crt", Resource.fromClasspath("ssl/ca.crt"))
        .rolesFile(Resource.fromClasspath("roles.yml"))
        .user("admin_user", "admin-password")
        .user("security_test_user", "security-test-password", "security_test_role")
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected boolean preserveClusterUponCompletion() {
        return true;
    }

    @Override
    protected Settings restAdminSettings() {
        String token = basicAuthHeaderValue("admin_user", new SecureString("admin-password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue("security_test_user", new SecureString("security-test-password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    @Override
    protected RestClient buildClient(Settings settings, HttpHost[] hosts) throws IOException {
        RestClientBuilder builder = RestClient.builder(hosts);
        configureClient(builder, settings);
        if (System.getProperty("tests.has_security") != null) {
            builder.setStrictDeprecationMode(true);
        } else {
            builder.setStrictDeprecationMode(false);
        }
        return builder.build();
    }

    @Order(1)
    public void testSecurityDisabledSetup() throws Exception {
        checkSecuritySetup(false);
    }

    @Order(2)
    public void testEnableSecurityAndRestartCluster() throws IOException {
        clusterSettings.put("xpack.security.enabled", "true");
        clusterSettings.put("xpack.security.authc.anonymous.roles", "anonymous");
        clusterSettings.put("xpack.security.transport.ssl.enabled", "true");
        clusterSettings.put("xpack.security.transport.ssl.certificate", "transport.crt");
        clusterSettings.put("xpack.security.transport.ssl.key", "transport.key");
        clusterSettings.put("xpack.security.transport.ssl.key_passphrase", "transport-password");
        clusterSettings.put("xpack.security.transport.ssl.certificate_authorities", "ca.crt");
        cluster.restart(false);
        closeClients();
    }

    @Order(3)
    public void testSecurityEnabledSetup() throws Exception {
        checkSecuritySetup(true);
    }

    public void checkSecuritySetup(boolean securityEnabled) throws Exception {
        logger.info("Security status: {}", securityEnabled);
        logger.info("Cluster:\n{}", getClusterInfo());
        logger.info("Indices:\n{}", getIndices());
        checkBasicLicenseType();

        checkSecurityStatus(securityEnabled);
        if (securityEnabled) {
            checkAuthentication();
        }

        checkAllowedWrite("index_allowed");
        // Security runs third, and should see the docs from the first two (non-security) runs
        // Security explicitly disabled runs second and should see the doc from the first (implicitly disabled) run
        final int expectedIndexCount = securityEnabled ? 2 : 1;
        checkIndexCount("index_allowed", expectedIndexCount);

        final String otherIndex = "index_" + randomAlphaOfLengthBetween(2, 6).toLowerCase(Locale.ROOT);
        if (securityEnabled) {
            checkDeniedWrite(otherIndex);
        } else {
            checkAllowedWrite(otherIndex);
        }
        checkSecurityDisabledWarning(securityEnabled);
    }

    public void checkSecurityDisabledWarning(boolean securityEnabled) throws Exception {
        final Request request = new Request("GET", "/_cat/indices");
        Response response = client().performRequest(request);
        List<String> warningHeaders = response.getWarnings();
        if (securityExplicitlySet || securityEnabled) {
            assertThat(warningHeaders, Matchers.empty());
        } else {
            assertThat(warningHeaders, Matchers.hasSize(1));
            assertThat(
                warningHeaders.get(0),
                containsString(
                    "Elasticsearch built-in security features are not enabled. Without authentication, your cluster could be "
                        + "accessible to anyone. See https://www.elastic.co/guide/en/elasticsearch/reference/"
                        + Version.CURRENT.major
                        + "."
                        + Version.CURRENT.minor
                        + "/security-minimal-setup.html to enable security."
                )
            );
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

    private void checkBasicLicenseType() throws Exception {
        assertBusy(() -> {
            try {
                Map<String, Object> license = getAsMap("/_license");
                assertThat(license, notNullValue());
                assertThat(ObjectPath.evaluate(license, "license.type"), equalTo("basic"));
            } catch (ResponseException e) {
                throw new AssertionError(e);
            }
        });
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
        // The anonymous role is granted by anonymous access enabled in build.gradle
        assertThat(ObjectPath.evaluate(auth, "roles"), contains("security_test_role", "anonymous"));
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
