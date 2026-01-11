/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.remotecluster;

import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

/**
 * Shared cluster definitions for multi-cluster search security tests with restricted trust.
 * <p>
 * Both clusters use SSL/TLS for transport layer security with CA-signed certificates.
 * Trust is restricted using the {@code trust.yml} configuration.
 * <p>
 * Use {@link #clusterRule()} to get the shared test rule for a test class.
 */
public final class Clusters {

    public static final String FULFILLING_CLUSTER_NAME = "fulfilling-cluster";
    public static final String QUERYING_CLUSTER_NAME = "querying-cluster";
    public static final String REMOTE_CLUSTER_ALIAS = "my_remote_cluster";

    public static final String TEST_USER = "test_user";
    public static final String TEST_PASSWORD = "x-pack-test-password";

    // Path to certificates in x-pack:plugin:core test resources (on classpath via testArtifact dependency)
    private static final String CERTS_PATH = "org/elasticsearch/xpack/security/transport/ssl/certs/simple/nodes/";

    private static final boolean PROXY_MODE = initProxyMode();
    private static final ElasticsearchCluster FULFILLING_CLUSTER = initFulfillingCluster();
    private static final ElasticsearchCluster QUERYING_CLUSTER = initQueryingCluster();
    private static final TestRule CLUSTER_RULE = RuleChain.outerRule(FULFILLING_CLUSTER).around(QUERYING_CLUSTER);

    /**
     * Get the shared fulfilling cluster (remote cluster where data is indexed).
     */
    public static ElasticsearchCluster fulfillingCluster() {
        return FULFILLING_CLUSTER;
    }

    /**
     * Get the shared querying cluster (local cluster that performs CCS).
     */
    public static ElasticsearchCluster queryingCluster() {
        return QUERYING_CLUSTER;
    }

    /**
     * Get the shared cluster rule. This rule ensures the fulfilling cluster
     * starts before the querying cluster, and both stay alive for all tests.
     */
    public static TestRule clusterRule() {
        return CLUSTER_RULE;
    }

    /**
     * Check if proxy mode is enabled for remote cluster connections.
     */
    public static boolean isProxyMode() {
        return PROXY_MODE;
    }

    private static boolean initProxyMode() {
        // Read from system property set by Gradle
        return Boolean.parseBoolean(System.getProperty("tests.remote.proxy.mode", "false"));
    }

    /**
     * Create the fulfilling cluster (remote cluster) where data is indexed.
     * Uses n1.c1 certificate (node 1, cluster 1).
     */
    private static ElasticsearchCluster initFulfillingCluster() {
        return ElasticsearchCluster.local()
            .name(FULFILLING_CLUSTER_NAME)
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
            .shared(true)
            .build();
    }

    /**
     * Create the querying cluster (local cluster) that connects to the fulfilling cluster via CCS.
     * Uses n1.c2 certificate (node 1, cluster 2).
     */
    private static ElasticsearchCluster initQueryingCluster() {
        var builder = ElasticsearchCluster.local()
            .name(QUERYING_CLUSTER_NAME)
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
            .user(TEST_USER, TEST_PASSWORD)
            .shared(true);

        if (PROXY_MODE) {
            builder.setting("cluster.remote." + REMOTE_CLUSTER_ALIAS + ".mode", "proxy")
                .setting(
                    "cluster.remote." + REMOTE_CLUSTER_ALIAS + ".proxy_address",
                    () -> "\"" + FULFILLING_CLUSTER.getTransportEndpoint(0) + "\""
                );
        } else {
            builder.setting(
                "cluster.remote." + REMOTE_CLUSTER_ALIAS + ".seeds",
                () -> "[\"" + FULFILLING_CLUSTER.getTransportEndpoint(0) + "\"]"
            );
        }

        return builder.build();
    }
}
