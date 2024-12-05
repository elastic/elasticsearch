/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.unconfigured_node_name;

import org.elasticsearch.common.logging.JsonLogsIntegTestCase;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.LogType;
import org.hamcrest.Matcher;
import org.junit.ClassRule;

import java.io.InputStream;

import static org.hamcrest.Matchers.equalTo;

public class JsonLogsFormatAndParseIT extends JsonLogsIntegTestCase {
    private static final String OS_NAME = System.getProperty("os.name");
    private static final boolean WINDOWS = OS_NAME.startsWith("Windows");

    private static final String COMPUTERNAME = "WindowsTestComputername";
    private static final String HOSTNAME = "LinuxDarwinTestHostname";

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .setting("xpack.security.enabled", "false")
        .setting("discovery.type", "single-node")
        .withNode(
            localNodeSpecBuilder -> localNodeSpecBuilder.withoutName()
                .environment("HOSTNAME", HOSTNAME)
                .environment("COMPUTERNAME", COMPUTERNAME)
        )
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected Matcher<String> nodeNameMatcher() {
        if (WINDOWS) {
            return equalTo(COMPUTERNAME);
        }
        return equalTo(HOSTNAME);
    }

    @Override
    protected InputStream openLogsStream() {
        return cluster.getNodeLog(0, LogType.SERVER_JSON);
    }
}
