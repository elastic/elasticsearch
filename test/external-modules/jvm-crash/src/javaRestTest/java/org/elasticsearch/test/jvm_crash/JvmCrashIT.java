/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.jvm_crash;

import org.apache.lucene.util.Constants;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.AbstractLocalClusterSpecBuilder;
import org.elasticsearch.test.cluster.local.DefaultEnvironmentProvider;
import org.elasticsearch.test.cluster.local.DefaultLocalClusterFactory;
import org.elasticsearch.test.cluster.local.DefaultLocalElasticsearchCluster;
import org.elasticsearch.test.cluster.local.DefaultSettingsProvider;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.local.distribution.LocalDistributionResolver;
import org.elasticsearch.test.cluster.local.distribution.ReleasedDistributionResolver;
import org.elasticsearch.test.cluster.local.distribution.SnapshotDistributionResolver;
import org.elasticsearch.test.cluster.util.OS;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.hamcrest.Matcher;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.matchesRegex;
import static org.hamcrest.Matchers.not;

public class JvmCrashIT extends ESRestTestCase {

    @BeforeClass
    public static void dontRunWindows() {
        assumeFalse("JVM crash log doesn't go to stdout on windows", OS.current() == OS.WINDOWS);
    }

    private static class StdOutCatchingClusterBuilder extends AbstractLocalClusterSpecBuilder<ElasticsearchCluster> {

        private StdOutCatchingClusterBuilder() {
            this.settings(new DefaultSettingsProvider());
            this.environment(new DefaultEnvironmentProvider());
        }

        @Override
        public ElasticsearchCluster build() {
            // redirect stdout before the nodes start up
            // they are referenced directly by ProcessUtils, so can't be changed afterwards
            redirectStdout();

            return new DefaultLocalElasticsearchCluster<>(
                this::buildClusterSpec,
                new DefaultLocalClusterFactory(
                    new LocalDistributionResolver(new SnapshotDistributionResolver(new ReleasedDistributionResolver()))
                )
            );
        }
    }

    private static PrintStream originalOut;
    private static ByteArrayOutputStream stdOutput;

    private static void redirectStdout() {
        if (originalOut == null) {
            originalOut = System.out;
            stdOutput = new ByteArrayOutputStream();
            // this duplicates the crash messages, but not the log output. That's ok.
            System.setOut(new TeePrintStream(originalOut, stdOutput));
        }
    }

    @ClassRule
    public static ElasticsearchCluster cluster = new StdOutCatchingClusterBuilder().distribution(DistributionType.INTEG_TEST)
        .nodes(1)
        .module("test-jvm-crash")
        .setting("xpack.security.enabled", "false")
        .jvmArg("-Djvm.crash=true")
        .build();

    @AfterClass
    public static void resetStdout() {
        if (originalOut != null) {
            System.setOut(originalOut);
        }
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public void testJvmCrash() throws Exception {
        final long pid = getElasticsearchPid();
        assertJvmArgs(pid, containsString("-Djvm.crash=true"));

        expectThrows(IOException.class, () -> client().performRequest(new Request("GET", "/_crash")));

        // the Elasticsearch process should die
        assertBusy(() -> assertJvmArgs(pid, not(containsString("-Djvm.crash=true"))));

        // parse the logs and ensure that Elasticsearch died with the expected cause
        assertThat(
            stdOutput,
            hasToString(
                matchesRegex(
                    Pattern.compile(".*# A fatal error has been detected by the Java Runtime Environment:.*SIGSEGV.*", Pattern.DOTALL)
                )
            )
        );
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

    @Override
    protected boolean preserveClusterUponCompletion() {
        // as the cluster is dead its state can not be wiped successfully so we have to bypass wiping the cluster
        return true;
    }
}
