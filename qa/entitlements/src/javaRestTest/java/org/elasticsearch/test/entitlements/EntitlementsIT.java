/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.entitlements;

import org.apache.lucene.util.Constants;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.hamcrest.Matcher;
import org.junit.ClassRule;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.containsString;

@ESTestCase.WithoutSecurityManager
public class EntitlementsIT extends ESRestTestCase {

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.INTEG_TEST)
        .plugin("entitlement-qa")
        .systemProperty("entitlement.enabled", "true")
        .setting("xpack.security.enabled", "false")
        .jvmArg("-Dentitlement.test=true")
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public void testCheckSystemExit() throws Exception {
        final long pid = getElasticsearchPid();
        assertJvmArgs(pid, containsString("-Dentitlement.test=true"));

        var exception = expectThrows(IOException.class, () -> client().performRequest(new Request("GET", "/_check_system_exit")));
        assertThat(exception.getMessage(), containsString("Not entitled"));
    }

    private Process startJcmd(long pid) throws IOException {
        final String jcmdPath = PathUtils.get(System.getProperty("java.home"), "bin/jcmd").toString();
        return new ProcessBuilder().command(jcmdPath, Long.toString(pid), "VM.command_line").redirectErrorStream(true).start();
    }

    private void assertJvmArgs(long pid, Matcher<String> matcher) throws IOException, InterruptedException {
        Process jcmdProcess = startJcmd(pid);

        if (Constants.WINDOWS) {
            // jcmd on windows appears to have a subtle bug where if the process being connected to
            // dies while jcmd is running, it can hang indefinitely. Here we detect this case by
            // waiting a fixed amount of time, and then killing/retrying the process
            boolean exited = jcmdProcess.waitFor(10, TimeUnit.SECONDS);
            if (exited == false) {
                logger.warn("jcmd hung, killing process and retrying");
                jcmdProcess.destroyForcibly();
                jcmdProcess = startJcmd(pid);
            }
        }

        List<String> outputLines = readLines(jcmdProcess.getInputStream());

        String jvmArgs = outputLines.stream().filter(l -> l.startsWith("jvm_args")).findAny().orElse(null);
        try {
            assertThat(jvmArgs, matcher);
        } catch (AssertionError ae) {
            logger.error("Failed matcher for jvm pid " + pid);
            logger.error("jcmd output: " + String.join("\n", outputLines));
            throw ae;
        }
    }

    private static long getElasticsearchPid() throws IOException {
        Response response = client().performRequest(new Request("GET", "/_nodes/process"));
        @SuppressWarnings("unchecked")
        var nodesInfo = (Map<String, Object>) entityAsMap(response).get("nodes");
        @SuppressWarnings("unchecked")
        var nodeInfo = (Map<String, Object>) nodesInfo.values().iterator().next();
        @SuppressWarnings("unchecked")
        var processInfo = (Map<String, Object>) nodeInfo.get("process");
        Object stringPid = processInfo.get("id");
        return Long.parseLong(stringPid.toString());
    }

    private static List<String> readLines(InputStream is) throws IOException {
        try (BufferedReader in = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
            return in.lines().toList();
        }
    }
}
