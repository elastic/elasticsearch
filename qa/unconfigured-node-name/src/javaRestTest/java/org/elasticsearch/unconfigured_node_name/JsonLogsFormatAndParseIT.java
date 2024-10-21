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

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;

public class JsonLogsFormatAndParseIT extends JsonLogsIntegTestCase {
    private static final String OS_NAME = System.getProperty("os.name");
    private static final boolean WINDOWS = OS_NAME.startsWith("Windows");

    // These match the values defined in org.elasticsearch.gradle.testclusters.ElasticsearchNode
    private static final String COMPUTERNAME = "WindowsComputername";
    private static final String HOSTNAME = "LinuxDarwinHostname";

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .setting("xpack.security.enabled", "false")
        .withNode(localNodeSpecBuilder -> localNodeSpecBuilder.name(OS_NAME.startsWith("Windows") ? COMPUTERNAME : HOSTNAME))
        // TODO @jozala -
        .settingsModifier(
            settings -> settings.entrySet()
                .stream()
                .filter(it -> it.getKey().equals("node.name") == false)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
        )
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected String getLogFileName() {
        // TODO @jozala REFACTOR - not needed anymore - delete it after refactoring
        return "server.log";
    }

    @Override
    protected Matcher<String> nodeNameMatcher() {
        if (WINDOWS) {
            return equalTo(COMPUTERNAME);
        }
        return equalTo(HOSTNAME);
    }

    @Override
    protected BufferedReader openReader(Path logFile) {
        return new BufferedReader(new InputStreamReader(cluster.getNodeLog(0, LogType.SERVER_JSON)));
    }
}
