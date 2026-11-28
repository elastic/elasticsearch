/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.run;

import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class RunClusterArgsTests {

    private static final File TLS_DIR = new File("/build-tools-internal/src/main/resources/run.ssl");

    @Test
    public void testBasicArguments() {
        List<String> args = baseArgs().numberOfNodes(3)
            .workingDir(new File("/es/build/testclusters"))
            .ports(9201, 9301, 9443)
            .pidFile(new File("/es/build/testclusters/runTask.pids"))
            .build();

        assertOption(args, "--name", "runTask");
        assertOption(args, "--distribution", "DEFAULT");
        assertOption(args, "--nodes", "3");
        assertOption(args, "--working-dir", new File("/es/build/testclusters").getAbsolutePath());
        assertOption(args, "--http-port", "9201");
        assertOption(args, "--transport-port", "9301");
        assertOption(args, "--remote-cluster-port", "9443");
        assertOption(args, "--pid-file", new File("/es/build/testclusters/runTask.pids").getAbsolutePath());
        assertFalse(args.contains("--preserve-data"));
        assertFalse(args.contains("--debug-server-jvm"));
        assertFalse(args.contains("--debug-cli-jvm"));
    }

    @Test
    public void testRemoteClusterPortIsOptional() {
        List<String> args = baseArgs().build();
        assertFalse(args.contains("--remote-cluster-port"));
    }

    @Test
    public void testConfiguration() {
        List<String> args = baseArgs().settings(Map.of("xpack.security.enabled", "true"))
            .systemProperties(Map.of("ingest.geoip.downloader.enabled.default", "true"))
            .jvmArgs(List.of("--add-opens=java.base/java.nio=ALL-UNNAMED"))
            .keystore(Map.of("bootstrap.password", "password"), Map.of("gcs.credentials_file", new File("/creds.json")), "secret")
            .users(List.of(new RunUser("elastic-admin", "elastic-password", "_es_test_root"), new RunUser("other", "pw", null)))
            .plugins(List.of("analysis-icu"))
            .modules(List.of("x-pack-eql"))
            .configFiles(Map.of("extra.yml", new File("/extra.yml")))
            .preserveData(true)
            .dataDir(new File("/data"))
            .build();

        assertOption(args, "--setting", "xpack.security.enabled=true");
        assertOption(args, "--system-property", "ingest.geoip.downloader.enabled.default=true");
        assertOption(args, "--jvm-arg", "--add-opens=java.base/java.nio=ALL-UNNAMED");
        assertOption(args, "--keystore", "bootstrap.password=password");
        assertOption(args, "--keystore-file", "gcs.credentials_file=" + new File("/creds.json").getAbsolutePath());
        assertOption(args, "--keystore-password", "secret");
        assertOption(args, "--user", "elastic-admin:elastic-password:_es_test_root");
        assertOption(args, "--user", "other:pw");
        assertOption(args, "--plugin", "analysis-icu");
        assertOption(args, "--module", "x-pack-eql");
        assertOption(args, "--config-file", "extra.yml=" + new File("/extra.yml").getAbsolutePath());
        assertOption(args, "--data-dir", new File("/data").getAbsolutePath());
        assertTrue(args.contains("--preserve-data"));
    }

    @Test
    public void testEmptyKeystorePasswordIsOmitted() {
        List<String> args = baseArgs().keystore(Map.of(), Map.of(), "").build();
        assertFalse(args.contains("--keystore-password"));
    }

    @Test
    public void testTelemetryDisabledByDefault() {
        List<String> args = baseArgs().build();
        assertOption(args, "--setting", "telemetry.metrics.enabled=false");
        assertOption(args, "--setting", "telemetry.tracing.enabled=false");
    }

    @Test
    public void testTelemetryNotConfiguredForIntegTestDistribution() {
        List<String> args = baseArgs().distribution("INTEG_TEST").build();
        assertFalse(args.contains("telemetry.metrics.enabled=false"));
        assertFalse(args.contains("telemetry.tracing.enabled=false"));
    }

    @Test
    public void testExplicitTelemetrySettingsAreKept() {
        List<String> args = baseArgs().settings(Map.of("telemetry.metrics.enabled", "true")).build();
        assertOption(args, "--setting", "telemetry.metrics.enabled=true");
        assertOption(args, "--setting", "telemetry.tracing.enabled=false");
        assertFalse(args.contains("telemetry.metrics.enabled=false"));
    }

    @Test
    public void testTelemetryLeftToCallerWhenEnabled() {
        List<String> args = baseArgs().telemetryEnabled(true).build();
        assertFalse(args.contains("telemetry.metrics.enabled=false"));
        assertFalse(args.contains("telemetry.tracing.enabled=false"));
    }

    @Test
    public void testHttps() {
        List<String> args = baseArgs().tls(true, false, TLS_DIR).build();

        assertOption(args, "--setting", "xpack.security.http.ssl.enabled=true");
        assertOption(args, "--setting", "xpack.security.http.ssl.keystore.path=https.keystore");
        assertOption(args, "--setting", "xpack.security.http.ssl.certificate_authorities=https.ca");
        assertOption(args, "--config-file", "https.keystore=" + new File(TLS_DIR, RunClusterArgs.HTTPS_CERTIFICATE).getAbsolutePath());
        assertOption(args, "--config-file", "https.ca=" + new File(TLS_DIR, RunClusterArgs.TLS_CERTIFICATE_AUTHORITY).getAbsolutePath());
        assertFalse(args.contains("xpack.security.transport.ssl.enabled=true"));
    }

    @Test
    public void testTransportTls() {
        List<String> args = baseArgs().tls(false, true, TLS_DIR).build();

        assertOption(args, "--setting", "xpack.security.transport.ssl.enabled=true");
        assertOption(args, "--setting", "xpack.security.transport.ssl.client_authentication=required");
        assertOption(
            args,
            "--config-file",
            "transport.keystore=" + new File(TLS_DIR, RunClusterArgs.TRANSPORT_CERTIFICATE).getAbsolutePath()
        );
        assertOption(
            args,
            "--config-file",
            "transport.ca=" + new File(TLS_DIR, RunClusterArgs.TLS_CERTIFICATE_AUTHORITY).getAbsolutePath()
        );
    }

    @Test
    public void testHttpsConflictsWithExplicitSettings() {
        IllegalArgumentException e = assertThrows(
            IllegalArgumentException.class,
            () -> baseArgs().settings(Map.of("xpack.security.http.ssl.verification_mode", "none")).tls(true, false, TLS_DIR).build()
        );
        assertEquals("Can not use --https with xpack.security.http.ssl.verification_mode", e.getMessage());
    }

    @Test
    public void testDebugging() {
        List<String> args = baseArgs().debugServer(true, 5008).debugCli(true, 5108).build();

        assertTrue(args.contains("--debug-server-jvm"));
        assertOption(args, "--debug-server-port", "5008");
        assertTrue(args.contains("--debug-cli-jvm"));
        assertOption(args, "--debug-cli-port", "5108");
    }

    @Test
    public void testSecurityEnabledByDefaultForDefaultDistribution() {
        assertOption(baseArgs().build(), "--setting", "xpack.security.enabled=true");
        List<String> args = baseArgs().settings(Map.of("xpack.security.enabled", "false")).build();
        assertOption(args, "--setting", "xpack.security.enabled=false");
        assertFalse(args.contains("xpack.security.enabled=true"));
        assertFalse(baseArgs().distribution("INTEG_TEST").build().contains("xpack.security.enabled=true"));
    }

    @Test
    public void testLicense() {
        List<String> args = baseArgs().settings(Map.of("xpack.license.self_generated.type", "basic")).license("trial").build();
        assertOption(args, "--setting", "xpack.license.self_generated.type=trial");
        assertFalse(args.contains("xpack.license.self_generated.type=basic"));
    }

    @Test
    public void testLicenseIsOptional() {
        assertFalse(baseArgs().build().stream().anyMatch(arg -> arg.startsWith("xpack.license.self_generated.type")));
    }

    @Test
    public void testLicenseRequiresDefaultDistribution() {
        IllegalArgumentException e = assertThrows(
            IllegalArgumentException.class,
            () -> baseArgs().distribution("INTEG_TEST").license("trial").build()
        );
        assertEquals("License type [trial] requires the default distribution", e.getMessage());
    }

    @Test
    public void testUnsupportedLicense() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> baseArgs().license("gold").build());
        assertEquals("Unsupported license type [gold], expected one of [basic, trial]", e.getMessage());
    }

    @Test
    public void testParseKeyValues() {
        Map<String, String> values = RunClusterArgs.parseKeyValues(
            "setting",
            List.of("cluster.name=foo", "cluster.remote.a.seeds=[\"127.0.0.1:9300\"]", "es.flag=a=b", "empty=", "cluster.name=bar")
        );
        assertEquals(
            Map.of("cluster.name", "bar", "cluster.remote.a.seeds", "[\"127.0.0.1:9300\"]", "es.flag", "a=b", "empty", ""),
            values
        );
    }

    @Test
    public void testParseKeyValuesRejectsInvalidValues() {
        IllegalArgumentException e = assertThrows(
            IllegalArgumentException.class,
            () -> RunClusterArgs.parseKeyValues("setting", List.of("no-separator"))
        );
        assertEquals("Invalid value for --setting [no-separator], expected <key>=<value>", e.getMessage());
        e = assertThrows(IllegalArgumentException.class, () -> RunClusterArgs.parseKeyValues("system-property", List.of("=value")));
        assertEquals("Invalid value for --system-property [=value], expected <key>=<value>", e.getMessage());
    }

    private static RunClusterArgs baseArgs() {
        return new RunClusterArgs().clusterName("runTask").distribution("DEFAULT").ports(9200, 9300, null);
    }

    /**
     * Asserts that the given option is passed with the given value at least once.
     */
    private static void assertOption(List<String> args, String option, String value) {
        List<String> values = new ArrayList<>();
        for (int i = 0; i < args.size() - 1; i++) {
            if (args.get(i).equals(option)) {
                values.add(args.get(i + 1));
            }
        }
        assertTrue("Expected " + option + " " + value + " in " + values, values.contains(value));
    }
}
