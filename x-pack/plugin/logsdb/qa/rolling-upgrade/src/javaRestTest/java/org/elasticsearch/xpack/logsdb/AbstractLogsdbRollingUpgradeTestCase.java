/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.util.Version;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.ClassRule;

import java.io.IOException;

public class AbstractLogsdbRollingUpgradeTestCase extends ESRestTestCase {
    private static final String USER = "admin-user";
    private static final String PASS = "x-pack-test-password";

    @ClassRule
    public static final ElasticsearchCluster cluster = Clusters.oldVersionCluster(USER, PASS);

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue(USER, new SecureString(PASS.toCharArray()));
        return Settings.builder().put(super.restClientSettings()).put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    protected void upgradeNode(int n) throws IOException {
        closeClients();
        var upgradeVersion = System.getProperty("tests.new_cluster_version") != null
            ? Version.fromString(System.getProperty("tests.new_cluster_version"))
            : Version.CURRENT;
        logger.info("Upgrading node {} to version {}", n, upgradeVersion);
        cluster.upgradeNodeToVersion(n, upgradeVersion);
        initClient();
    }
}
