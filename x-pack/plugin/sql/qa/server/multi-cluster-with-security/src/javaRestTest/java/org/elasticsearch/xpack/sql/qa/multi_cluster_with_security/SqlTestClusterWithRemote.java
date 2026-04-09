/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.qa.multi_cluster_with_security;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.io.IOException;

import static org.elasticsearch.test.rest.ESRestTestCase.basicAuthHeaderValue;
import static org.elasticsearch.xpack.sql.qa.rest.RemoteClusterAwareSqlRestTestCase.clientBuilder;

public class SqlTestClusterWithRemote implements TestRule {
    public static final String LOCAL_CLUSTER_NAME = "javaRestTest";
    public static final String REMOTE_CLUSTER_NAME = "remote-cluster";
    public static final String REMOTE_CLUSTER_ALIAS = "my_remote_cluster";
    public static final String USER_NAME = "test_user";
    public static final String PASSWORD = "x-pack-test-password";

    private static ElasticsearchCluster clusterSettings(String remoteAddress) {
        return ElasticsearchCluster.local()
            .distribution(DistributionType.DEFAULT)
            .name(LOCAL_CLUSTER_NAME)
            .setting("xpack.ml.enabled", "false")
            .setting("xpack.watcher.enabled", "false")
            .setting("cluster.remote." + REMOTE_CLUSTER_ALIAS + ".seeds", remoteAddress)
            .setting("cluster.remote.connections_per_cluster", "1")
            .setting("xpack.security.enabled", "true")
            .setting("xpack.license.self_generated.type", "trial")
            .setting("xpack.security.autoconfiguration.enabled", "false")
            .user(USER_NAME, PASSWORD)
            .build();
    }

    private static ElasticsearchCluster remoteClusterSettings() {
        return ElasticsearchCluster.local()
            .distribution(DistributionType.DEFAULT)
            .name(REMOTE_CLUSTER_NAME)
            .setting("node.roles", "[data,ingest,master]")
            .setting("xpack.ml.enabled", "false")
            .setting("xpack.watcher.enabled", "false")
            .setting("xpack.security.enabled", "true")
            .setting("xpack.license.self_generated.type", "trial")
            .setting("xpack.security.autoconfiguration.enabled", "false")
            .user(USER_NAME, PASSWORD)
            .build();
    }

    /**
     * Auth settings for both the cluster and the remote.
     */
    private static Settings clientAuthSettings() {
        final String value = basicAuthHeaderValue(USER_NAME, new SecureString(PASSWORD.toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", value).build();
    }

    private ElasticsearchCluster cluster;
    private final ElasticsearchCluster remote = remoteClusterSettings();
    private RestClient remoteClient;

    public Statement apply(Statement base, Description description) {
        return remote.apply(startRemoteClient(startCluster(base)), null);
    }

    public ElasticsearchCluster getCluster() {
        return cluster;
    }

    public Settings clusterAuthSettings() {
        return clientAuthSettings();
    }

    public RestClient getRemoteClient() {
        return remoteClient;
    }

    private Statement startCluster(Statement base) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                // Remote address will look like [::1]:12345 - elasticsearch.yml does not like the square brackets.
                String remoteAddress = remote.getTransportEndpoint(0).replaceAll("\\[|\\]", "");
                cluster = clusterSettings(remoteAddress);
                cluster.apply(base, null).evaluate();
            }
        };
    }

    private Statement startRemoteClient(Statement base) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                try {
                    remoteClient = initRemoteClient();
                    base.evaluate();
                } finally {
                    IOUtils.close(remoteClient);
                }
            }
        };
    }

    private RestClient initRemoteClient() throws IOException {
        String crossClusterHost = remote.getHttpAddress(0);
        int portSeparator = crossClusterHost.lastIndexOf(':');
        if (portSeparator < 0) {
            throw new IllegalArgumentException("Illegal cluster url [" + crossClusterHost + "]");
        }
        String host = crossClusterHost.substring(0, portSeparator);
        int port = Integer.parseInt(crossClusterHost.substring(portSeparator + 1));
        HttpHost[] remoteHttpHosts = new HttpHost[] { new HttpHost(host, port) };

        return clientBuilder(clientAuthSettings(), remoteHttpHosts);
    }
}
