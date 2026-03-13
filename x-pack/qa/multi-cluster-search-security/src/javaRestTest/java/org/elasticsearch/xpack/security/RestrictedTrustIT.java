/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.xpack.security;

import com.carrotsearch.randomizedtesting.annotations.TestCaseOrdering;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

@TestCaseOrdering(TestCaseOrdering.AlphabeticOrder.class)
public class RestrictedTrustIT extends ESRestTestCase {

    private static final String TEST_USER = "test_user";
    private static final String TEST_PASSWORD = "x-pack-test-password";

    private static final String REMOTE_CLUSTER_ALIAS = "my_remote_cluster";

    // Path to certificates in x-pack:plugin:core test resources (on classpath via testArtifact dependency)
    private static final String CERTS_PATH = "org/elasticsearch/xpack/security/transport/ssl/certs/simple/nodes/";

    private static final ElasticsearchCluster fulfillingCluster = ElasticsearchCluster.local()
        .name("fulfilling-cluster")
        .distribution(DistributionType.DEFAULT)
        .setting("xpack.security.enabled", "true")
        .setting("xpack.license.self_generated.type", "basic")
        // Transport SSL configuration
        .setting("xpack.security.transport.ssl.enabled", "true")
        .setting("xpack.security.transport.ssl.client_authentication", "required")
        .setting("xpack.security.transport.ssl.key", "transport.key")
        .setting("xpack.security.transport.ssl.certificate", "transport.cert")
        .setting("xpack.security.transport.ssl.certificate_authorities", "transport.ca")
        .setting("xpack.security.transport.ssl.verification_mode", "certificate")
        .setting("xpack.security.transport.ssl.trust_restrictions.path", "trust.yml")
        // Certificate files for cluster 1 (c1)
        .configFile("transport.key", Resource.fromClasspath(CERTS_PATH + "ca-signed/n1.c1.key"))
        .configFile("transport.cert", Resource.fromClasspath(CERTS_PATH + "ca-signed/n1.c1.crt"))
        .configFile("transport.ca", Resource.fromClasspath(CERTS_PATH + "ca.crt"))
        .configFile("trust.yml", Resource.fromClasspath("trust.yml"))
        .user(TEST_USER, TEST_PASSWORD)
        .build();

    private static final ElasticsearchCluster queryingCluster = ElasticsearchCluster.local()
        .name("querying-cluster")
        .distribution(DistributionType.DEFAULT)
        .setting("xpack.security.enabled", "true")
        .setting("xpack.license.self_generated.type", "basic")
        // Transport SSL configuration
        .setting("xpack.security.transport.ssl.enabled", "true")
        .setting("xpack.security.transport.ssl.client_authentication", "required")
        .setting("xpack.security.transport.ssl.key", "transport.key")
        .setting("xpack.security.transport.ssl.certificate", "transport.cert")
        .setting("xpack.security.transport.ssl.certificate_authorities", "transport.ca")
        .setting("xpack.security.transport.ssl.verification_mode", "certificate")
        .setting("xpack.security.transport.ssl.trust_restrictions.path", "trust.yml")
        .setting("xpack.security.transport.ssl.trust_restrictions.x509_fields", "subjectAltName.dnsName")
        // Certificate files for cluster 2 (c2)
        .configFile("transport.key", Resource.fromClasspath(CERTS_PATH + "ca-signed/n1.c2.key"))
        .configFile("transport.cert", Resource.fromClasspath(CERTS_PATH + "ca-signed/n1.c2.crt"))
        .configFile("transport.ca", Resource.fromClasspath(CERTS_PATH + "ca.crt"))
        .configFile("trust.yml", Resource.fromClasspath("trust.yml"))
        // Remote cluster settings
        .setting("cluster.remote.connections_per_cluster", "1")
        .setting("cluster.remote." + REMOTE_CLUSTER_ALIAS + ".skip_unavailable", "false")
        .settings(spec -> {
            final Map<String, String> settings = new HashMap<>();
            if (randomBoolean()) {
                settings.put("cluster.remote." + REMOTE_CLUSTER_ALIAS + ".mode", "proxy");
                settings.put(
                    "cluster.remote." + REMOTE_CLUSTER_ALIAS + ".proxy_address",
                    "\"" + fulfillingCluster.getTransportEndpoint(0) + "\""
                );
            } else {
                settings.put("cluster.remote." + REMOTE_CLUSTER_ALIAS + ".mode", "sniff");
                settings.put("cluster.remote." + REMOTE_CLUSTER_ALIAS + ".seeds", "\"" + fulfillingCluster.getTransportEndpoint(0) + "\"");
            }
            return settings;
        })
        .user(TEST_USER, TEST_PASSWORD)
        .build();

    @ClassRule
    public static TestRule clusterRule = RuleChain.outerRule(fulfillingCluster).around(queryingCluster);

    private void resetClients() throws Exception {
        closeClients();
        initClient();
    }

    @Override
    protected boolean preserveIndicesUponCompletion() {
        return true;
    }

    @Override
    protected boolean preserveDataStreamsUponCompletion() {
        return true;
    }

    @Override
    protected String getTestRestCluster() {
        return isUsingFulfillingCluster() ? fulfillingCluster.getHttpAddresses() : queryingCluster.getHttpAddresses();
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue(TEST_USER, new SecureString(TEST_PASSWORD.toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    // runs first based on alphabetic test method ordering
    public void test10SetupTestData() throws Exception {
        resetClients();
        Request indexDocRequest = new Request("POST", "/test_idx/_doc?refresh=true");
        indexDocRequest.setJsonEntity("{\"foo\": \"bar\"}");
        Response response = client().performRequest(indexDocRequest);
        assertOK(response);
    }

    // runs second based on alphabetic test method ordering
    public void test20RemoteAccessPortFunctions() throws Exception {
        resetClients();
        Request searchRequest = new Request("GET", "/" + REMOTE_CLUSTER_ALIAS + ":test_idx/_search");
        Response response = client().performRequest(searchRequest);
        assertOK(response);
        ObjectPath responseObj = ObjectPath.createFromResponse(response);
        int totalHits = responseObj.evaluate("hits.total.value");
        assertThat(totalHits, equalTo(1));
    }

    private boolean isUsingFulfillingCluster() {
        return getTestName().contains("Setup");
    }

}
