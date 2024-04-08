/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.qa.multi_cluster_with_security;

import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.sql.qa.rest.RestSqlTestCase;
import org.junit.ClassRule;

import static org.elasticsearch.transport.RemoteClusterAware.buildRemoteIndexName;
import static org.elasticsearch.xpack.sql.qa.multi_cluster_with_security.SqlTestClusterWithRemote.REMOTE_CLUSTER_ALIAS;

public class RestSqlIT extends RestSqlTestCase {
    @ClassRule
    public static SqlTestClusterWithRemote clusterAndRemote = new SqlTestClusterWithRemote();

    @Override
    protected String getTestRestCluster() {
        return clusterAndRemote.getCluster().getHttpAddresses();
    }

    @Override
    protected Settings restClientSettings() {
        return clusterAndRemote.clusterAuthSettings();
    }

    @Override
    protected RestClient provisioningClient() {
        return clusterAndRemote.getRemoteClient();
    }

    @Override
    protected String indexPattern(String pattern) {
        if (randomBoolean()) {
            return buildRemoteIndexName(REMOTE_CLUSTER_ALIAS, pattern);
        } else {
            String cluster = REMOTE_CLUSTER_ALIAS.substring(0, randomIntBetween(0, REMOTE_CLUSTER_ALIAS.length())) + "*";
            if (pattern.startsWith("\\\"") && pattern.endsWith("\\\"") && pattern.length() > 4) {
                pattern = pattern.substring(2, pattern.length() - 2);
            }
            return "\\\"" + buildRemoteIndexName(cluster, pattern) + "\\\""; // rest tests don't do JSON escaping
        }
    }
}
