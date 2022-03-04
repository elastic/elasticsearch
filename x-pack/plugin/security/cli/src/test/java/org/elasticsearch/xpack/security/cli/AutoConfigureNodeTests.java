/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.cli;

import joptsimple.OptionParser;

import org.apache.commons.io.FileUtils;
import org.bouncycastle.asn1.x509.GeneralName;
import org.elasticsearch.cli.MockTerminal;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.KeyStoreWrapper;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.ssl.KeyStoreUtil;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.http.HttpTransportSettings;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.cert.X509Certificate;
import java.util.List;

import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static org.elasticsearch.xpack.security.cli.AutoConfigureNode.anyRemoteHostNodeAddress;
import static org.elasticsearch.xpack.security.cli.AutoConfigureNode.removePreviousAutoconfiguration;
import static org.hamcrest.Matchers.equalTo;
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
        // test no publish settings
        Path tempDir = createTempDir();
        try {
            Files.createDirectory(tempDir.resolve("config"));
            // empty yml file, it just has to exist
            Files.write(tempDir.resolve("config").resolve("elasticsearch.yml"), List.of(), CREATE_NEW);
            X509Certificate httpCertificate = runAutoConfigAndReturnHTTPCertificate(tempDir, Settings.EMPTY);
            assertThat(checkGeneralNameSan(httpCertificate, "dummy.test.hostname", GeneralName.dNSName), is(true));
            assertThat(checkGeneralNameSan(httpCertificate, "localhost", GeneralName.dNSName), is(true));
        } finally {
            deleteDirectory(tempDir);
        }

        // test network publish settings
        tempDir = createTempDir();
        try {
            Files.createDirectory(tempDir.resolve("config"));
            // empty yml file, it just has to exist
            Files.write(tempDir.resolve("config").resolve("elasticsearch.yml"), List.of(), CREATE_NEW);
            X509Certificate httpCertificate = runAutoConfigAndReturnHTTPCertificate(
                tempDir,
                Settings.builder()
                    .put(NetworkService.GLOBAL_NETWORK_PUBLISH_HOST_SETTING.getKey(), "172.168.1.100")
                    .put(HttpTransportSettings.SETTING_HTTP_HOST.getKey(), "10.10.10.100")
                    .build()
            );
            assertThat(checkGeneralNameSan(httpCertificate, "dummy.test.hostname", GeneralName.dNSName), is(true));
            assertThat(checkGeneralNameSan(httpCertificate, "localhost", GeneralName.dNSName), is(true));
            assertThat(checkGeneralNameSan(httpCertificate, "172.168.1.100", GeneralName.iPAddress), is(true));
            assertThat(checkGeneralNameSan(httpCertificate, "10.10.10.100", GeneralName.iPAddress), is(false));
        } finally {
            deleteDirectory(tempDir);
        }

        // test http publish settings
        tempDir = createTempDir();
        try {
            Files.createDirectory(tempDir.resolve("config"));
            // empty yml file, it just has to exist
            Files.write(tempDir.resolve("config").resolve("elasticsearch.yml"), List.of(), CREATE_NEW);
            X509Certificate httpCertificate = runAutoConfigAndReturnHTTPCertificate(
                tempDir,
                Settings.builder()
                    .put(NetworkService.GLOBAL_NETWORK_HOST_SETTING.getKey(), "172.168.1.100")
                    .put(HttpTransportSettings.SETTING_HTTP_PUBLISH_HOST.getKey(), "10.10.10.100")
                    .build()
            );
            assertThat(checkGeneralNameSan(httpCertificate, "dummy.test.hostname", GeneralName.dNSName), is(true));
            assertThat(checkGeneralNameSan(httpCertificate, "localhost", GeneralName.dNSName), is(true));
            assertThat(checkGeneralNameSan(httpCertificate, "172.168.1.100", GeneralName.iPAddress), is(false));
            assertThat(checkGeneralNameSan(httpCertificate, "10.10.10.100", GeneralName.iPAddress), is(true));
        } finally {
            deleteDirectory(tempDir);
        }

        // test network AND http publish settings
        tempDir = createTempDir();
        try {
            Files.createDirectory(tempDir.resolve("config"));
            // empty yml file, it just has to exist
            Files.write(tempDir.resolve("config").resolve("elasticsearch.yml"), List.of(), CREATE_NEW);
            X509Certificate httpCertificate = runAutoConfigAndReturnHTTPCertificate(
                tempDir,
                Settings.builder()
                    .put(NetworkService.GLOBAL_NETWORK_PUBLISH_HOST_SETTING.getKey(), "gypsy.hill")
                    .put(NetworkService.GLOBAL_NETWORK_HOST_SETTING.getKey(), "172.168.1.100")
                    .put(HttpTransportSettings.SETTING_HTTP_PUBLISH_HOST.getKey(), "balkan.beast")
                    .put(HttpTransportSettings.SETTING_HTTP_HOST.getKey(), "10.10.10.100")
                    .build()
            );
            assertThat(checkGeneralNameSan(httpCertificate, "dummy.test.hostname", GeneralName.dNSName), is(true));
            assertThat(checkGeneralNameSan(httpCertificate, "localhost", GeneralName.dNSName), is(true));
            assertThat(checkGeneralNameSan(httpCertificate, "gypsy.hill", GeneralName.dNSName), is(true));
            assertThat(checkGeneralNameSan(httpCertificate, "balkan.beast", GeneralName.dNSName), is(true));
            assertThat(checkGeneralNameSan(httpCertificate, "172.168.1.100", GeneralName.iPAddress), is(false));
            assertThat(checkGeneralNameSan(httpCertificate, "10.10.10.100", GeneralName.iPAddress), is(false));
        } finally {
            deleteDirectory(tempDir);
        }
    }

    public void testAnyRemoteHostNodeAddress() throws Exception {
        List<String> remoteAddresses = List.of("192.168.0.1:9300", "127.0.0.1:9300");
        InetAddress[] localAddresses = new InetAddress[] { InetAddress.getByName("192.168.0.1"), InetAddress.getByName("127.0.0.1") };
        assertThat(anyRemoteHostNodeAddress(remoteAddresses, localAddresses), equalTo(false));

        remoteAddresses = List.of("192.168.0.1:9300", "127.0.0.1:9300", "[::1]:9300");
        localAddresses = new InetAddress[] { InetAddress.getByName("192.168.0.1"), InetAddress.getByName("127.0.0.1") };
        assertThat(anyRemoteHostNodeAddress(remoteAddresses, localAddresses), equalTo(false));

        remoteAddresses = List.of("192.168.0.1:9300", "127.0.0.1:9300", "[::1]:9300");
        localAddresses = new InetAddress[] {
            InetAddress.getByName("192.168.0.1"),
            InetAddress.getByName("127.0.0.1"),
            InetAddress.getByName("10.0.0.1") };
        assertThat(anyRemoteHostNodeAddress(remoteAddresses, localAddresses), equalTo(false));

        remoteAddresses = List.of("192.168.0.1:9300", "127.0.0.1:9300", "[::1]:9300", "10.0.0.1:9301");
        localAddresses = new InetAddress[] { InetAddress.getByName("192.168.0.1"), InetAddress.getByName("127.0.0.1") };
        assertThat(anyRemoteHostNodeAddress(remoteAddresses, localAddresses), equalTo(true));

        remoteAddresses = List.of("127.0.0.1:9300", "[::1]:9300");
        localAddresses = new InetAddress[] { InetAddress.getByName("[::1]"), InetAddress.getByName("127.0.0.1") };
        assertThat(anyRemoteHostNodeAddress(remoteAddresses, localAddresses), equalTo(false));

        remoteAddresses = List.of("127.0.0.1:9300", "[::1]:9300");
        localAddresses = new InetAddress[] { InetAddress.getByName("192.168.2.3") };
        assertThat(anyRemoteHostNodeAddress(remoteAddresses, localAddresses), equalTo(false));

        remoteAddresses = List.of("1.2.3.4:9300");
        localAddresses = new InetAddress[] { InetAddress.getByName("[::1]"), InetAddress.getByName("127.0.0.1") };
        assertThat(anyRemoteHostNodeAddress(remoteAddresses, localAddresses), equalTo(true));

        remoteAddresses = List.of();
        localAddresses = new InetAddress[] { InetAddress.getByName("192.168.0.1"), InetAddress.getByName("127.0.0.1") };
        assertThat(anyRemoteHostNodeAddress(remoteAddresses, localAddresses), equalTo(false));
    }

    private boolean checkGeneralNameSan(X509Certificate certificate, String generalName, int generalNameTag) throws Exception {
        for (List<?> san : certificate.getSubjectAlternativeNames()) {
            if (san.get(0).equals(generalNameTag) && san.get(1).equals(generalName)) {
                return true;
            }
        }
        return false;
    }

    private X509Certificate runAutoConfigAndReturnHTTPCertificate(Path configDir, Settings settings) throws Exception {
        final Environment env = TestEnvironment.newEnvironment(Settings.builder().put("path.home", configDir).put(settings).build());
        // runs the command to auto-generate the config files and the keystore
        new AutoConfigureNode().execute(new MockTerminal(), new OptionParser().parse(), env);

        KeyStoreWrapper nodeKeystore = KeyStoreWrapper.load(configDir.resolve("config"));
        nodeKeystore.decrypt(new char[0]); // the keystore is always bootstrapped with an empty password

        SecureString httpKeystorePassword = nodeKeystore.getString("xpack.security.http.ssl.keystore.secure_password");

        final Settings newSettings = Settings.builder().loadFromPath(env.configFile().resolve("elasticsearch.yml")).build();
        final String httpKeystorePath = newSettings.get("xpack.security.http.ssl.keystore.path");

        KeyStore httpKeystore = KeyStoreUtil.readKeyStore(
            configDir.resolve("config").resolve(httpKeystorePath),
            "PKCS12",
            httpKeystorePassword.getChars()
        );
        return (X509Certificate) httpKeystore.getCertificate("http");
    }

    @SuppressForbidden(reason = "Uses File API because the commons io library does, which is useful for file manipulation")
    private void deleteDirectory(Path directory) throws IOException {
        FileUtils.deleteDirectory(directory.toFile());
    }
}
