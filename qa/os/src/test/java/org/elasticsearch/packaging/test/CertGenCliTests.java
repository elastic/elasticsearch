/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.packaging.test;

import org.apache.http.client.fluent.Request;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.packaging.util.FileUtils;
import org.elasticsearch.packaging.util.Platforms;
import org.elasticsearch.packaging.util.ServerUtils;
import org.elasticsearch.packaging.util.Shell;
import org.junit.Before;
import org.junit.BeforeClass;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static com.carrotsearch.randomizedtesting.RandomizedTest.assumeFalse;
import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static org.elasticsearch.packaging.util.FileMatcher.Fileness.File;
import static org.elasticsearch.packaging.util.FileMatcher.file;
import static org.elasticsearch.packaging.util.FileMatcher.p600;
import static org.elasticsearch.packaging.util.FileUtils.escapePath;
import static org.hamcrest.CoreMatchers.containsString;

public class CertGenCliTests extends PackagingTestCase {
    private static final Path instancesFile = getRootTempDir().resolve("instances.yml");
    private static final Path certificatesFile = getRootTempDir().resolve("certificates.zip");

    @Before
    public void filterDistros() {
        assumeFalse("no docker", distribution.isDocker());
    }

    @BeforeClass
    public static void cleanupFiles() {
        FileUtils.rm(instancesFile, certificatesFile);
    }

    public void test10Install() throws Exception {
        install();
        // Disable security auto-configuration as we want to generate keys/certificates manually here
        ServerUtils.disableSecurityAutoConfiguration(installation);
    }

    public void test20Help() {
        Shell.Result result = installation.executables().certgenTool.run("--help");
        assertThat(result.stdout(), containsString("Simplifies certificate creation"));
    }

    public void test30Generate() throws Exception {
        final List<String> lines = new ArrayList<>();
        lines.add("instances:");
        lines.add("  - name: \"mynode\"");
        lines.add("    ip:");
        lines.add("      - \"127.0.0.1\"");
        Files.write(instancesFile, lines, CREATE, APPEND);

        installation.executables().certgenTool.run("--in " + instancesFile + " --out " + certificatesFile);

        String owner = installation.getOwner();
        assertThat(certificatesFile, file(File, owner, owner, p600));
    }

    public void test31ExtractCerts() throws Exception {
        // windows 2012 r2 has powershell 4.0, which lacks Expand-Archive
        assumeFalse(Platforms.OS_NAME.equals("Windows Server 2012 R2"));

        Path certsDir = installation.config("certs");
        sh.extractZip(certificatesFile, certsDir);

        Path caDir = certsDir.resolve("ca");
        assertThat(caDir.resolve("ca.key"), file(File, null, null, null));
        assertThat(caDir.resolve("ca.crt"), file(File, null, null, null));

        Path nodeDir = certsDir.resolve("mynode");
        assertThat(nodeDir.resolve("mynode.key"), file(File, null, null, null));
        assertThat(nodeDir.resolve("mynode.crt"), file(File, null, null, null));

        FileUtils.cp(certsDir, installation.config("certs"));
    }

    public void test40RunWithCert() throws Exception {
        // windows 2012 r2 has powershell 4.0, which lacks Expand-Archive
        assumeFalse(Platforms.OS_NAME.equals("Windows Server 2012 R2"));

        final String keyPath = escapePath(installation.config("certs/mynode/mynode.key"));
        final String certPath = escapePath(installation.config("certs/mynode/mynode.crt"));
        final String caCertPath = escapePath(installation.config("certs/ca/ca.crt"));
        final Settings settings = Settings.builder().loadFromPath(installation.config("elasticsearch.yml")).build();
        // Replace possibly auto-configured TLS settings with ones pointing to the material generated with certgen
        // (we do disable auto-configuration above but for packaged installations TLS auto-config happens on installation time and is
        // not affected by this setting
        final Settings newSettings = Settings.builder()
            .put(
                settings.filter(k -> k.startsWith("xpack.security") == false)
                    .filter(k -> k.equals("node.name") == false)
                    .filter(k -> k.equals("http.host") == false)
                    .filter(k -> k.equals("cluster.initial_master_nodes") == false)

            )
            .put("node.name", "mynode")
            .put("xpack.security.transport.ssl.key", keyPath)
            .put("xpack.security.transport.ssl.certificate", certPath)
            .put("xpack.security.transport.ssl.certificate_authorities", caCertPath)
            .put("xpack.security.http.ssl.key", keyPath)
            .put("xpack.security.http.ssl.certificate", certPath)
            .putList("xpack.security.http.ssl.certificate_authorities", caCertPath)
            .put("xpack.security.transport.ssl.enabled", true)
            .put("xpack.security.http.ssl.enabled", true)
            .build();
        Files.write(
            installation.config("elasticsearch.yml"),
            newSettings.keySet().stream().map(k -> k + ": " + newSettings.get(k)).collect(Collectors.toList()),
            TRUNCATE_EXISTING
        );

        assertWhileRunning(() -> {
            final String password = setElasticPassword();
            assertNotNull(password);
            ServerUtils.makeRequest(Request.Get("https://127.0.0.1:9200"), "elastic", password, installation.config("certs/ca/ca.crt"));
        });
    }

    private String setElasticPassword() {
        Shell.Result result = installation.executables().resetPasswordTool.run("--auto --batch --silent --username elastic", null);
        return result.stdout();
    }

}
