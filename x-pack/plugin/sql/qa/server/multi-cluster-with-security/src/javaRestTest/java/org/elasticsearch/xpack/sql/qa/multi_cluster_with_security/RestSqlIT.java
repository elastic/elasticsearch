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
import org.elasticsearch.xpack.sql.qa.rest.RestSqlTestCase;
import org.junit.ClassRule;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.io.IOException;

import static org.elasticsearch.transport.RemoteClusterAware.buildRemoteIndexName;

public class RestSqlIT extends RestSqlTestCase {
    @ClassRule
    public static TestClusterWithRemote clusterAndRemote = new TestClusterWithRemote();

    public static class TestClusterWithRemote implements TestRule {
        private ElasticsearchCluster cluster;
        private final ElasticsearchCluster remote = SqlTestRemoteCluster.getCluster();
        private RestClient remoteClient;

        public Statement apply(Statement base, Description description) {
            return remote.apply(startRemoteClient(startCluster(base)), null);
        }

        private Statement startRemoteClient(Statement base) {
            return new Statement() {
                @Override
                public void evaluate() throws Throwable {
                    try {
                        remoteClient = initRemoteClient();
                        base.evaluate();
                    }
                    finally {
                        IOUtils.close(remoteClient);
                    }
                }
            };
        }

        private Statement startCluster(Statement base) {
            return new Statement() {
                @Override
                public void evaluate() throws Throwable {
                    // Remote address will look like [::1]:12345 - elasticsearch.yml does not like the square brackets.
                    String remoteAddress = remote.getTransportEndpoint(0).replaceAll("\\[|\\]", "");
                    cluster = SqlTestCluster.getCluster(remoteAddress);
                    cluster.apply(base, null).evaluate();
                }
            };
        }

        public ElasticsearchCluster cluster() {
            return cluster;
        }

        public ElasticsearchCluster remote() {
            return remote;
        }

        public Settings clusterAuthSettings() {
            return clientAuthSettings();
        }

        public RestClient remoteClient() {
            return remoteClient;
        }

        public Settings remoteAuthSettings() {
            return clientAuthSettings();
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

        /**
         * Auth settings for both the cluster and the remote.
         */
        private static Settings clientAuthSettings() {
            final String value = basicAuthHeaderValue("test_user", new SecureString("x-pack-test-password".toCharArray()));
            return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", value).build();
        }
    }

    @Override
    protected String getTestRestCluster() {
        return clusterAndRemote.cluster().getHttpAddress(0);
    }

    @Override
    protected Settings restClientSettings() {
        return clusterAndRemote.clusterAuthSettings();
    }

    @Override
    protected RestClient provisioningClient() {
        return clusterAndRemote.remoteClient();
    }

    public static final String REMOTE_CLUSTER_NAME = "my_remote_cluster";

    @Override
    protected String indexPattern(String pattern) {
        if (randomBoolean()) {
            return buildRemoteIndexName(REMOTE_CLUSTER_NAME, pattern);
        } else {
            String cluster = REMOTE_CLUSTER_NAME.substring(0, randomIntBetween(0, REMOTE_CLUSTER_NAME.length())) + "*";
            if (pattern.startsWith("\\\"") && pattern.endsWith("\\\"") && pattern.length() > 4) {
                pattern = pattern.substring(2, pattern.length() - 2);
            }
            return "\\\"" + buildRemoteIndexName(cluster, pattern) + "\\\""; // rest tests don't do JSON escaping
        }
    }
}
