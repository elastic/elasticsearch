/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.common.logging;

import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.LogType;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.hamcrest.Matchers;
import org.junit.ClassRule;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.hamcrest.Matchers.matchesRegex;

/**
 * This test verifies that Elasticsearch can startup successfully with a custom logging config using variables introduced in
 * <code>ESJsonLayout</code>
 * The intention is to confirm that users can still run their Elasticsearch instances with previous configurations.
 */
public class CustomLoggingConfigIT extends ESRestTestCase {
    // we are looking for a line where pattern contains:
    // [2020-03-20T14:51:59,989][INFO ][o.e.g.GatewayService ] [integTest-0] recovered [0] indices into cluster_state
    private static final String NODE_STARTED = ".*recovered.*cluster_state.*";

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .setting("xpack.security.enabled", "false")
        .configFile("log4j2.properties", Resource.fromClasspath("es-v7-log4j2.properties"))
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public void testSuccessfulStartupWithCustomConfig() throws Exception {
        assertBusy(() -> {
            List<String> lines = getPlaintextLog();
            assertThat(lines, Matchers.hasItem(matchesRegex(NODE_STARTED)));
        });
    }

    public void testParseAllV7JsonLines() throws Exception {
        assertBusy(() -> {
            List<String> lines = getJSONLog();
            assertThat(lines, Matchers.hasItem(matchesRegex(NODE_STARTED)));
        });
    }

    private List<String> getJSONLog() {
        try (InputStream nodeLog = cluster.getNodeLog(0, LogType.SERVER_JSON)) {
            return new BufferedReader(new InputStreamReader(nodeLog, StandardCharsets.UTF_8)).lines().toList();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private List<String> getPlaintextLog() {
        try (InputStream nodeLog = cluster.getNodeLog(0, LogType.SERVER)) {
            return new BufferedReader(new InputStreamReader(nodeLog, StandardCharsets.UTF_8)).lines().toList();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
