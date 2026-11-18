/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.cluster.cli;

import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.local.model.User;
import org.junit.Test;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThrows;

public class ClusterRunnerArgsTests {

    @Test
    public void testDefaults() {
        ClusterRunnerArgs args = ClusterRunnerArgs.parse();

        assertThat(args.name(), is("runTask"));
        assertThat(args.distribution(), is(DistributionType.DEFAULT));
        assertThat(args.nodes(), is(1));
        assertThat(args.httpPort(), is(9200));
        assertThat(args.transportPort(), is(9300));
        assertThat(args.remoteClusterPort(), nullValue());
        assertThat(args.workingDir(), nullValue());
        assertThat(args.preserveData(), is(false));
        assertThat(args.debugServer(), is(false));
        assertThat(args.debugServerPort(), is(5007));
        assertThat(args.debugCli(), is(false));
        assertThat(args.debugCliPort(), is(5107));
        assertThat(args.users(), empty());
        assertThat(args.help(), is(false));
    }

    @Test
    public void testClusterOptions() {
        ClusterRunnerArgs args = ClusterRunnerArgs.parse(
            "--name",
            "querying-cluster",
            "--distribution",
            "integ_test",
            "--nodes",
            "3",
            "--working-dir",
            "/tmp/clusters",
            "--preserve-data",
            "--data-dir",
            "/tmp/data",
            "--http-port",
            "9201",
            "--transport-port",
            "9301",
            "--remote-cluster-port",
            "9443",
            "--pid-file",
            "/tmp/clusters/pids"
        );

        assertThat(args.name(), is("querying-cluster"));
        assertThat(args.distribution(), is(DistributionType.INTEG_TEST));
        assertThat(args.nodes(), is(3));
        assertThat(args.workingDir(), is(Path.of("/tmp/clusters")));
        assertThat(args.preserveData(), is(true));
        assertThat(args.dataDir(), is(Path.of("/tmp/data")));
        assertThat(args.httpPort(), is(9201));
        assertThat(args.transportPort(), is(9301));
        assertThat(args.remoteClusterPort(), is(9443));
        assertThat(args.pidFile(), is(Path.of("/tmp/clusters/pids")));
        assertThat(args.nodeName(2), is("querying-cluster-2"));
    }

    @Test
    public void testRepeatableKeyValueOptions() {
        ClusterRunnerArgs args = ClusterRunnerArgs.parse(
            "--setting",
            "xpack.security.enabled=true",
            "--setting",
            "cluster.remote.my_remote_cluster.seeds=[\"127.0.0.1:9300\"]",
            "--system-property",
            "es.flag=a=b",
            "--keystore",
            "bootstrap.password=password",
            "--keystore-file",
            "gcs.credentials_file=/tmp/creds.json",
            "--config-file",
            "https.ca=/tmp/ca.pem",
            "--jvm-arg",
            "--add-opens=java.base/java.nio=ALL-UNNAMED",
            "--jvm-arg",
            "-Dfoo=bar"
        );

        assertThat(
            args.settings(),
            is(Map.of("xpack.security.enabled", "true", "cluster.remote.my_remote_cluster.seeds", "[\"127.0.0.1:9300\"]"))
        );
        // values are split on the first '=' only
        assertThat(args.systemProperties(), is(Map.of("es.flag", "a=b")));
        assertThat(args.keystoreSettings(), is(Map.of("bootstrap.password", "password")));
        assertThat(args.keystoreFiles(), is(Map.of("gcs.credentials_file", Path.of("/tmp/creds.json"))));
        assertThat(args.configFiles(), is(Map.of("https.ca", Path.of("/tmp/ca.pem"))));
        assertThat(args.jvmArgs(), contains("--add-opens=java.base/java.nio=ALL-UNNAMED", "-Dfoo=bar"));
    }

    @Test
    public void testNodeSettings() {
        ClusterRunnerArgs args = ClusterRunnerArgs.parse(
            "--node-setting",
            "0:node.roles=[]",
            "--node-setting",
            "1:node.roles=[master,data_hot]",
            "--node-setting",
            "1:node.attr.zone=b",
            "--nodes",
            "2"
        );

        assertThat(args.nodeSettings().get(0), is(Map.of("node.roles", "[]")));
        assertThat(args.nodeSettings().get(1), is(Map.of("node.roles", "[master,data_hot]", "node.attr.zone", "b")));
    }

    @Test
    public void testPluginsAndModules() {
        ClusterRunnerArgs args = ClusterRunnerArgs.parse("--plugin", "analysis-icu", "--module", "x-pack-eql", "--plugin", "my-plugin");

        assertThat(args.plugins(), contains("analysis-icu", "my-plugin"));
        assertThat(args.modules(), contains("x-pack-eql"));
    }

    @Test
    public void testUsers() {
        ClusterRunnerArgs args = ClusterRunnerArgs.parse("--user", "elastic-admin:elastic-password", "--user", "admin:secret:superuser");

        List<User> users = args.users();
        assertThat(users.size(), is(2));
        assertThat(users.get(0).getUsername(), is("elastic-admin"));
        assertThat(users.get(0).getPassword(), is("elastic-password"));
        assertThat(users.get(0).getRole(), is(User.ROOT_USER_ROLE));
        assertThat(users.get(1).getUsername(), is("admin"));
        assertThat(users.get(1).getPassword(), is("secret"));
        assertThat(users.get(1).getRole(), is("superuser"));
    }

    @Test
    public void testDebugOptions() {
        ClusterRunnerArgs args = ClusterRunnerArgs.parse(
            "--debug-server-jvm",
            "--debug-server-port",
            "5008",
            "--debug-cli-jvm",
            "--debug-cli-port",
            "5108"
        );

        assertThat(args.debugServer(), is(true));
        assertThat(args.debugServerPort(), is(5008));
        assertThat(args.debugCli(), is(true));
        assertThat(args.debugCliPort(), is(5108));
    }

    @Test
    public void testHelp() {
        assertThat(ClusterRunnerArgs.parse("--help").help(), is(true));
        assertThat(ClusterRunnerArgs.usage(), containsString("starting ports"));
    }

    @Test
    public void testInvalidArguments() {
        assertError("Unknown option [--bogus]", "--bogus");
        assertError("Missing value for option [--name]", "--name");
        assertError("expected <key>=<value>", "--setting", "no-separator");
        assertError("expected <key>=<value>", "--setting", "=value");
        assertError("expected <name>:<password>[:<role>]", "--user", "only-a-name");
        assertError("expected <name>:<password>[:<role>]", "--user", "a:b:c:d");
        assertError("expected DEFAULT or INTEG_TEST", "--distribution", "oss");
        assertError("expected a number", "--nodes", "three");
        assertError("must be at least 1", "--nodes", "0");
        assertError("must be a valid port", "--http-port", "70000");
        assertError("expected <index>:<key>=<value>", "--node-setting", "node.roles=[]");
        assertError("expected <index>:<key>=<value>", "--node-setting", "x:node.roles=[]");
        assertError("expected <key>=<value>", "--node-setting", "0:node.roles");
        assertError("the cluster has 1 node(s)", "--node-setting", "1:node.roles=[]");
    }

    private static void assertError(String expectedMessage, String... argv) {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> ClusterRunnerArgs.parse(argv));
        assertThat(e.getMessage(), containsString(expectedMessage));
    }
}
