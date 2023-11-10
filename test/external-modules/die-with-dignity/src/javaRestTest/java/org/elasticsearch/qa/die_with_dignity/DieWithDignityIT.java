/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.qa.die_with_dignity;

import org.apache.lucene.util.Constants;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.LogType;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.hamcrest.Matcher;
import org.junit.ClassRule;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;

public class DieWithDignityIT extends ESRestTestCase {

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.INTEG_TEST)
        .module("test-die-with-dignity")
        .setting("xpack.security.enabled", "false")
        .jvmArg("-Ddie.with.dignity.test=true")
        .jvmArg("-XX:-ExitOnOutOfMemoryError")
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public void testDieWithDignity() throws Exception {
        final long pid = getElasticsearchPid();
        assertJvmArgs(pid, containsString("-Ddie.with.dignity.test=true"));

        expectThrows(IOException.class, () -> client().performRequest(new Request("GET", "/_die_with_dignity")));

        // the Elasticsearch process should die
        assertBusy(() -> assertJvmArgs(pid, not(containsString("-Ddie.with.dignity.test=true"))));

        // parse the logs and ensure that Elasticsearch died with the expected cause
        boolean fatalError = false;
        boolean fatalErrorInThreadExiting = false;
        for (String line : readLines(cluster.getNodeLog(0, LogType.SERVER_JSON))) {
            if (containsAll(line, ".*ERROR.*", ".*ExceptionsHelper.*", ".*fatal error.*")) {
                fatalError = true;
            } else if (containsAll(
                line,
                ".*ERROR.*",
                ".*ElasticsearchUncaughtExceptionHandler.*",
                ".*fatal error in thread \\[elasticsearch-error-rethrower\\], exiting.*",
                ".*java.lang.OutOfMemoryError: Requested array size exceeds VM limit.*"
            )) {
                fatalErrorInThreadExiting = true;
            }
        }
        assertTrue(fatalError);
        assertTrue(fatalErrorInThreadExiting);
    }

    private Process startJcmd(long pid) throws IOException {
        final String jcmdPath = PathUtils.get(System.getProperty("tests.runtime.java"), "bin/jcmd").toString();
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

        String jvmArgs = null;
        try {
            for (String line : outputLines) {
                if (line.startsWith("jvm_args")) {
                    jvmArgs = line;
                    break;
                }
            }
            assertThat(jvmArgs, matcher);

        } catch (AssertionError ae) {
            logger.error("Failed matcher for jvm pid " + pid);
            logger.error("jcmd output: " + String.join("\n", outputLines));
            throw ae;
        }
    }

    private long getElasticsearchPid() throws IOException {
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

    private List<String> readLines(InputStream is) throws IOException {
        try (BufferedReader in = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
            return in.lines().toList();
        }
    }

    private boolean containsAll(String line, String... subStrings) {
        for (String subString : subStrings) {
            if (line.matches(subString) == false) {
                return false;
            }
        }
        return true;
    }

    private void debugLogs(Path path) throws IOException {
        try (BufferedReader reader = Files.newBufferedReader(path)) {
            reader.lines().forEach(line -> logger.info(line));
        }
    }

    @Override
    protected boolean preserveClusterUponCompletion() {
        // as the cluster is dead its state can not be wiped successfully so we have to bypass wiping the cluster
        return true;
    }

}
