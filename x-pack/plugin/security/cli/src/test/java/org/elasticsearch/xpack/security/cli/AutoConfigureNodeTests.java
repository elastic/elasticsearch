/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.cli;

import joptsimple.OptionParser;

import org.bouncycastle.asn1.x509.GeneralName;
import org.elasticsearch.cli.MockTerminal;
import org.elasticsearch.common.settings.KeyStoreWrapper;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.ssl.KeyStoreUtil;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.test.ESTestCase;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.cert.X509Certificate;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static org.elasticsearch.xpack.security.cli.AutoConfigureNode.removePreviousAutoconfiguration;
import static org.hamcrest.Matchers.is;

public class AutoConfigureNodeTests extends ESTestCase {

    public void testRemovePreviousAutoconfiguration() throws Exception {
        final List<String> file1 = List.of(
            "# commented out line",
            "# commented out line",
            "# commented out line",
            "# commented out line",
            "# commented out line",
            "# commented out line",
            "some.setting1: some.value",
            "some.setting2: some.value",
            "some.setting3: some.value",
            "some.setting4: some.value",
            "# commented out line",
            "# commented out line",
            "# commented out line"
        );
        final List<String> file2 = List.of(
            "# commented out line",
            "# commented out line",
            "# commented out line",
            "# commented out line",
            "# commented out line",
            "# commented out line",
            "some.setting1: some.value",
            "some.setting2: some.value",
            "some.setting3: some.value",
            "some.setting4: some.value",
            "# commented out line",
            "# commented out line",
            "# commented out line",
            AutoConfigureNode.AUTO_CONFIGURATION_START_MARKER,
            "cluster.initial_master_nodes: [\"node1\"]",
            "http.host: [_site_]",
            "xpack.security.enabled: true",
            "xpack.security.enrollment.enabled: true",
            "xpack.security.http.ssl.enabled: true",
            "xpack.security.http.ssl.keystore.path: /path/to/the/file",
            "xpack.security.transport.ssl.keystore.path: /path/to/the/file",
            "xpack.security.transport.ssl.truststore.path: /path/to/the/file",
            AutoConfigureNode.AUTO_CONFIGURATION_END_MARKER
        );

        assertEquals(file1, removePreviousAutoconfiguration(file2));
    }

    public void testRemovePreviousAutoconfigurationRetainsUserAdded() throws Exception {
        final List<String> file1 = List.of(
            "# commented out line",
            "# commented out line",
            "# commented out line",
            "# commented out line",
            "# commented out line",
            "# commented out line",
            "some.setting1: some.value",
            "some.setting2: some.value",
            "some.setting3: some.value",
            "some.setting4: some.value",
            "# commented out line",
            "# commented out line",
            "# commented out line",
            "some.extra.added.setting: value"
        );
        final List<String> file2 = List.of(
            "# commented out line",
            "# commented out line",
            "# commented out line",
            "# commented out line",
            "# commented out line",
            "# commented out line",
            "some.setting1: some.value",
            "some.setting2: some.value",
            "some.setting3: some.value",
            "some.setting4: some.value",
            "# commented out line",
            "# commented out line",
            "# commented out line",
            AutoConfigureNode.AUTO_CONFIGURATION_START_MARKER,
            "cluster.initial_master_nodes: [\"node1\"]",
            "http.host: [_site_]",
            "xpack.security.enabled: true",
            "xpack.security.enrollment.enabled: true",
            "xpack.security.http.ssl.enabled: true",
            "some.extra.added.setting: value",
            "xpack.security.http.ssl.keystore.path: /path/to/the/file",
            "xpack.security.transport.ssl.keystore.path: /path/to/the/file",
            "xpack.security.transport.ssl.truststore.path: /path/to/the/file",
            "",
            AutoConfigureNode.AUTO_CONFIGURATION_END_MARKER
        );
        assertEquals(file1, removePreviousAutoconfiguration(file2));
    }

    public void testGeneratedHTTPCertificateSANs() throws Exception {
        final Path tempDir = createTempDir();
        Files.createDirectory(tempDir.resolve("config"));
        // empty yml file
        Files.write(tempDir.resolve("config").resolve("elasticsearch.yml"), List.of(), CREATE_NEW);
        X509Certificate httpCertificate = runAutoConfigAndReturnHTTPCertificate(tempDir);

        AtomicBoolean sanContainsHostname = new AtomicBoolean(false);
        AtomicBoolean sanContainsLocalhost = new AtomicBoolean(false);
        httpCertificate.getSubjectAlternativeNames().forEach(subjectAltName -> {
            if (subjectAltName.get(1).equals("dummy.test.hostname") &&
                subjectAltName.get(0).equals(GeneralName.dNSName)) {
                sanContainsHostname.set(true);
            }
            if (subjectAltName.get(1).equals("localhost") &&
                subjectAltName.get(0).equals(GeneralName.dNSName)) {
                sanContainsLocalhost.set(true);
            }
        });

        assertThat(sanContainsHostname.get(), is(true));
        assertThat(sanContainsLocalhost.get(), is(true));
    }

    private X509Certificate runAutoConfigAndReturnHTTPCertificate(Path configDir) throws Exception {
        final Environment env = TestEnvironment.newEnvironment(Settings.builder().put("path.home", configDir).build());
        // runs the command to auto-generate the config files and the keystore
        new AutoConfigureNode().execute(new MockTerminal(), new OptionParser().parse(), env);

        KeyStoreWrapper nodeKeystore = KeyStoreWrapper.load(configDir.resolve("config"));
        nodeKeystore.decrypt(new char[0]); // the keystore is always bootstrapped with an empty password

        SecureString httpKeystorePassword = nodeKeystore.getString("xpack.security.http.ssl.keystore.secure_password");

        List<String> generatedConfigLines = Files.readAllLines(env.configFile().resolve("elasticsearch.yml"), StandardCharsets.UTF_8);
        String httpKeystorePath = null;
        for (String generatedConfigLine : generatedConfigLines) {
            if (generatedConfigLine.startsWith("xpack.security.http.ssl.keystore.path")) {
                httpKeystorePath = generatedConfigLine.substring(39);
                break;
            }
        }

        KeyStore httpKeystore = KeyStoreUtil.readKeyStore(Path.of(httpKeystorePath), "PKCS12", httpKeystorePassword.getChars());
        return (X509Certificate) httpKeystore.getCertificate("http_local_node_key");
    }
}
