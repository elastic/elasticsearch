/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.cli;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.KeyPurposeId;
import org.elasticsearch.cli.MockTerminal;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.KeyStoreWrapper;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.http.HttpTransportSettings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.CommandLineHttpClient;
import org.elasticsearch.xpack.core.security.EnrollmentToken;
import org.elasticsearch.xpack.core.security.HttpResponse;
import org.elasticsearch.xpack.security.cli.bc.CertGenUtils;
import org.elasticsearch.xpack.security.cli.bc.KeyStoreFormat;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Base64;
import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;

import javax.security.auth.x500.X500Principal;

import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static org.elasticsearch.xpack.security.cli.AutoConfigureNode.AUTO_CONFIG_HTTP_ALT_DN;
import static org.elasticsearch.xpack.security.cli.AutoConfigureNode.AUTO_CONFIG_TRANSPORT_ALT_DN;
import static org.elasticsearch.xpack.security.cli.AutoConfigureNode.anyRemoteHostNodeAddress;
import static org.elasticsearch.xpack.security.cli.AutoConfigureNode.removePreviousAutoconfiguration;
import static org.elasticsearch.xpack.security.cli.CertGenUtilsTests.assertExpectedKeyUsage;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

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

    public void testSubjectAndIssuerForGeneratedCertificates() throws Exception {
        // test no publish settings
        Path tempDir = createTempDir();
        try {
            Files.createDirectory(tempDir.resolve("config"));
            // empty yml file, it just has to exist
            Files.write(tempDir.resolve("config").resolve("elasticsearch.yml"), List.of(), CREATE_NEW);
            Tuple<X509Certificate, X509Certificate> generatedCerts = runAutoConfigAndReturnCertificates(tempDir, Settings.EMPTY);
            assertThat(checkSubjectAndIssuerDN(generatedCerts.v1(), "CN=dummy.test.hostname", AUTO_CONFIG_HTTP_ALT_DN), is(true));
            assertThat(checkSubjectAndIssuerDN(generatedCerts.v2(), "CN=dummy.test.hostname", AUTO_CONFIG_TRANSPORT_ALT_DN), is(true));
        } finally {
            deleteDirectory(tempDir);
        }
    }

    /**
     * A node configured for FIPS mode is expected to run with an approved-only FIPS provider, which cannot open PKCS#12.
     * Auto-configuration must therefore emit BCFKS keystores (and matching paths) for such a node, while remaining
     * PKCS#12 for everything else. The container choice is driven solely by the setting, not by the JVM running the tool.
     */
    public void testKeyStoreFormatFollowsFipsModeSetting() throws Exception {
        final boolean fipsMode = randomBoolean();
        final Path tempDir = createTempDir();
        try {
            Files.createDirectory(tempDir.resolve("config"));
            Files.write(tempDir.resolve("config").resolve("elasticsearch.yml"), List.of(), CREATE_NEW);
            final Settings settings = Settings.builder().put(XPackSettings.FIPS_MODE_ENABLED.getKey(), fipsMode).build();
            // the helper asserts the configured paths and keystore types match the expected container for these settings
            Tuple<X509Certificate, X509Certificate> generatedCerts = runAutoConfigAndReturnCertificates(tempDir, settings);
            assertThat(checkSubjectAndIssuerDN(generatedCerts.v1(), "CN=dummy.test.hostname", AUTO_CONFIG_HTTP_ALT_DN), is(true));
            assertThat(checkSubjectAndIssuerDN(generatedCerts.v2(), "CN=dummy.test.hostname", AUTO_CONFIG_TRANSPORT_ALT_DN), is(true));

            final Path certsDir = tempDir.resolve("config").resolve("certs");
            final String expectedExtension = fipsMode ? ".bcfks" : ".p12";
            final String unexpectedExtension = fipsMode ? ".p12" : ".bcfks";
            assertThat(Files.exists(certsDir.resolve("http" + expectedExtension)), is(true));
            assertThat(Files.exists(certsDir.resolve("transport" + expectedExtension)), is(true));
            assertThat(Files.exists(certsDir.resolve("http" + unexpectedExtension)), is(false));
            assertThat(Files.exists(certsDir.resolve("transport" + unexpectedExtension)), is(false));
        } finally {
            deleteDirectory(tempDir);
        }
    }

    public void testGeneratedHTTPCertificateSANsAndKeyUsage() throws Exception {
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
            verifyKeyUsageAndExtendedKeyUsage(httpCertificate);
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
            verifyKeyUsageAndExtendedKeyUsage(httpCertificate);
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
            verifyKeyUsageAndExtendedKeyUsage(httpCertificate);
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

    public void testAutoConfiguresEncryptionKeys() throws Exception {
        Path tempDir = createTempDir();
        try {
            Files.createDirectory(tempDir.resolve("config"));
            Files.write(tempDir.resolve("config").resolve("elasticsearch.yml"), List.of(), CREATE_NEW);
            final Environment env = TestEnvironment.newEnvironment(Settings.builder().put("path.home", tempDir).build());
            new AutoConfigureNode(false).execute(MockTerminal.create(), new OptionParser().parse(), env, null);

            KeyStoreWrapper nodeKeystore = KeyStoreWrapper.load(tempDir.resolve("config"));
            nodeKeystore.decrypt(new char[0]);

            assertThat(
                nodeKeystore.getSettingNames()
                    .contains("cluster.state.encryption.password." + AutoConfigureNode.AUTO_CONFIG_ENCRYPTION_PASSWORD_ID),
                is(true)
            );
            try (SecureString activeId = nodeKeystore.getString("cluster.state.encryption.active_password_id")) {
                assertThat(activeId.toString(), equalTo(AutoConfigureNode.AUTO_CONFIG_ENCRYPTION_PASSWORD_ID));
            }
        } finally {
            deleteDirectory(tempDir);
        }
    }

    public void testEachNodeGetsADistinctEncryptionKey() throws Exception {
        final String passwordKey = "cluster.state.encryption.password." + AutoConfigureNode.AUTO_CONFIG_ENCRYPTION_PASSWORD_ID;
        Path tempDir1 = createTempDir();
        Path tempDir2 = createTempDir();
        try {
            for (Path dir : List.of(tempDir1, tempDir2)) {
                Files.createDirectory(dir.resolve("config"));
                Files.write(dir.resolve("config").resolve("elasticsearch.yml"), List.of(), CREATE_NEW);
                Environment env = TestEnvironment.newEnvironment(Settings.builder().put("path.home", dir).build());
                new AutoConfigureNode(false).execute(MockTerminal.create(), new OptionParser().parse(), env, null);
            }
            String password1;
            KeyStoreWrapper ks1 = KeyStoreWrapper.load(tempDir1.resolve("config"));
            ks1.decrypt(new char[0]);
            try (SecureString p = ks1.getString(passwordKey)) {
                password1 = p.toString();
            }
            KeyStoreWrapper ks2 = KeyStoreWrapper.load(tempDir2.resolve("config"));
            ks2.decrypt(new char[0]);
            try (SecureString p = ks2.getString(passwordKey)) {
                assertThat(p.toString(), not(equalTo(password1)));
            }
        } finally {
            deleteDirectory(tempDir1);
            deleteDirectory(tempDir2);
        }
    }

    public void testEnrollingNodeAutoConfiguresEncryptionKey() throws Exception {
        final KeyPair httpCaKeyPair = CertGenUtils.generateKeyPair(2048);
        final X509Certificate httpCaCert = CertGenUtils.generateSignedCertificate(
            new X500Principal("CN=test-http-ca"),
            null,
            httpCaKeyPair,
            null,
            null,
            true,
            365,
            "SHA256withRSA",
            null,
            Set.of()
        );
        final KeyPair transportCaKeyPair = CertGenUtils.generateKeyPair(2048);
        final X509Certificate transportCaCert = CertGenUtils.generateSignedCertificate(
            new X500Principal("CN=test-transport-ca"),
            null,
            transportCaKeyPair,
            null,
            null,
            true,
            365,
            "SHA256withRSA",
            null,
            Set.of()
        );
        final KeyPair transportKeyPair = CertGenUtils.generateKeyPair(2048);
        final X509Certificate transportCert = CertGenUtils.generateSignedCertificate(
            new X500Principal("CN=test-node"),
            null,
            transportKeyPair,
            transportCaCert,
            transportCaKeyPair.getPrivate(),
            false,
            365,
            "SHA256withRSA",
            null,
            Set.of()
        );

        final String responseJson = "{"
            + "\"http_ca_key\":\""
            + Base64.getEncoder().encodeToString(httpCaKeyPair.getPrivate().getEncoded())
            + "\","
            + "\"http_ca_cert\":\""
            + Base64.getEncoder().encodeToString(httpCaCert.getEncoded())
            + "\","
            + "\"transport_ca_cert\":\""
            + Base64.getEncoder().encodeToString(transportCaCert.getEncoded())
            + "\","
            + "\"transport_key\":\""
            + Base64.getEncoder().encodeToString(transportKeyPair.getPrivate().getEncoded())
            + "\","
            + "\"transport_cert\":\""
            + Base64.getEncoder().encodeToString(transportCert.getEncoded())
            + "\","
            + "\"nodes_addresses\":[\"127.0.0.1:9300\"]"
            + "}";

        final EnrollmentToken token = new EnrollmentToken("test-key-id:test-secret", "a".repeat(64), List.of("127.0.0.1:9200"));
        final String encodedToken = token.getEncoded();

        final BiFunction<Environment, String, CommandLineHttpClient> mockClientFn = (e, fp) -> new CommandLineHttpClient(e, fp) {
            @Override
            public HttpResponse execute(
                String method,
                URL url,
                SecureString apiKey,
                CheckedSupplier<String, Exception> requestBodySupplier,
                CheckedFunction<InputStream, HttpResponse.HttpResponseBuilder, Exception> responseHandler
            ) throws Exception {
                return responseHandler.apply(new ByteArrayInputStream(responseJson.getBytes(StandardCharsets.UTF_8)))
                    .withHttpStatus(200)
                    .build();
            }
        };

        // Local subclass to access protected parser for enrollment-mode options parsing
        class EnrollmentAutoConfigureNode extends AutoConfigureNode {
            EnrollmentAutoConfigureNode() {
                super(false, mockClientFn);
            }

            OptionSet parseArgs(String... args) {
                return parser.parse(args);
            }
        }

        Path tempDir = createTempDir();
        try {
            Files.createDirectory(tempDir.resolve("config"));
            Files.write(tempDir.resolve("config").resolve("elasticsearch.yml"), List.of(), CREATE_NEW);
            final Environment env = TestEnvironment.newEnvironment(Settings.builder().put("path.home", tempDir).build());

            EnrollmentAutoConfigureNode autoConfigureNode = new EnrollmentAutoConfigureNode();
            autoConfigureNode.execute(MockTerminal.create(), autoConfigureNode.parseArgs("--enrollment-token", encodedToken), env, null);

            KeyStoreWrapper nodeKeystore = KeyStoreWrapper.load(tempDir.resolve("config"));
            nodeKeystore.decrypt(new char[0]);

            assertThat(
                nodeKeystore.getSettingNames()
                    .contains("cluster.state.encryption.password." + AutoConfigureNode.AUTO_CONFIG_ENCRYPTION_PASSWORD_ID),
                is(true)
            );
            try (SecureString activeId = nodeKeystore.getString("cluster.state.encryption.active_password_id")) {
                assertThat(activeId.toString(), equalTo(AutoConfigureNode.AUTO_CONFIG_ENCRYPTION_PASSWORD_ID));
            }
        } finally {
            deleteDirectory(tempDir);
        }
    }

    public void testReconfigureNodeRemovesAutoconfiguredEncryptionKeys() throws Exception {
        final String passwordKey = "cluster.state.encryption.password." + AutoConfigureNode.AUTO_CONFIG_ENCRYPTION_PASSWORD_ID;
        Path tempDir = createTempDir();
        try {
            Files.createDirectory(tempDir.resolve("config"));
            KeyStoreWrapper ks = KeyStoreWrapper.create();
            for (String setting : List.of(
                "xpack.security.transport.ssl.keystore.secure_password",
                "xpack.security.transport.ssl.truststore.secure_password",
                "xpack.security.http.ssl.keystore.secure_password",
                "autoconfiguration.password_hash"
            )) {
                ks.setString(setting, "test".toCharArray());
            }
            ks.setString(passwordKey, "original-password".toCharArray());
            ks.setString("cluster.state.encryption.active_password_id", AutoConfigureNode.AUTO_CONFIG_ENCRYPTION_PASSWORD_ID.toCharArray());
            ks.save(tempDir.resolve("config"), new char[0]);

            final Environment env = TestEnvironment.newEnvironment(Settings.builder().put("path.home", tempDir).build());
            AutoConfigureNode.removeAutoConfigurationFromKeystore(env, MockTerminal.create());

            KeyStoreWrapper result = KeyStoreWrapper.load(tempDir.resolve("config"));
            result.decrypt(new char[0]);
            assertThat(result.getSettingNames().contains(passwordKey), is(false));
            assertThat(result.getSettingNames().contains("cluster.state.encryption.active_password_id"), is(false));
        } finally {
            deleteDirectory(tempDir);
        }
    }

    public void testReconfigureNodePreservesUserManagedEncryptionKeys() throws Exception {
        final String userPasswordKey = "cluster.state.encryption.password.my-hsm-key";
        Path tempDir = createTempDir();
        try {
            Files.createDirectory(tempDir.resolve("config"));
            KeyStoreWrapper ks = KeyStoreWrapper.create();
            for (String setting : List.of(
                "xpack.security.transport.ssl.keystore.secure_password",
                "xpack.security.transport.ssl.truststore.secure_password",
                "xpack.security.http.ssl.keystore.secure_password",
                "autoconfiguration.password_hash"
            )) {
                ks.setString(setting, "test".toCharArray());
            }
            ks.setString(userPasswordKey, "user-managed-secret".toCharArray());
            ks.setString("cluster.state.encryption.active_password_id", "my-hsm-key".toCharArray());
            ks.save(tempDir.resolve("config"), new char[0]);

            final Environment env = TestEnvironment.newEnvironment(Settings.builder().put("path.home", tempDir).build());
            AutoConfigureNode.removeAutoConfigurationFromKeystore(env, MockTerminal.create());

            KeyStoreWrapper result = KeyStoreWrapper.load(tempDir.resolve("config"));
            result.decrypt(new char[0]);
            assertThat(result.getSettingNames().contains(userPasswordKey), is(true));
            assertThat(result.getSettingNames().contains("cluster.state.encryption.active_password_id"), is(true));
            try (SecureString activeId = result.getString("cluster.state.encryption.active_password_id")) {
                assertThat(activeId.toString(), equalTo("my-hsm-key"));
            }
        } finally {
            deleteDirectory(tempDir);
        }
    }

    private boolean checkGeneralNameSan(X509Certificate certificate, String generalName, int generalNameTag) throws Exception {
        for (List<?> san : certificate.getSubjectAlternativeNames()) {
            if (san.get(0).equals(generalNameTag) && san.get(1).equals(generalName)) {
                return true;
            }
        }
        return false;
    }

    private boolean checkSubjectAndIssuerDN(X509Certificate certificate, String subjectName, String issuerName) throws Exception {
        if (certificate.getSubjectX500Principal().getName().equals(subjectName)
            && certificate.getIssuerX500Principal().getName().equals(issuerName)) {
            return true;
        }
        return false;
    }

    private void verifyKeyUsageAndExtendedKeyUsage(X509Certificate httpCertificate) throws Exception {
        List<String> extendedKeyUsage = httpCertificate.getExtendedKeyUsage();
        assertEquals("Only one extended key usage expected for HTTP certificate.", 1, extendedKeyUsage.size());
        String expectedServerAuthUsage = KeyPurposeId.id_kp_serverAuth.toASN1Primitive().toString();
        assertEquals("Expected serverAuth extended key usage.", expectedServerAuthUsage, extendedKeyUsage.get(0));
        assertExpectedKeyUsage(httpCertificate, HttpCertificateCommand.DEFAULT_CERT_KEY_USAGE);
    }

    private X509Certificate runAutoConfigAndReturnHTTPCertificate(Path configDir, Settings settings) throws Exception {
        Tuple<X509Certificate, X509Certificate> generatedCertificates = runAutoConfigAndReturnCertificates(configDir, settings);
        return generatedCertificates.v1();
    }

    private Tuple<X509Certificate, X509Certificate> runAutoConfigAndReturnCertificates(Path configDir, Settings settings) throws Exception {
        final Environment env = TestEnvironment.newEnvironment(Settings.builder().put("path.home", configDir).put(settings).build());
        // runs the command to auto-generate the config files and the keystore
        new AutoConfigureNode(false).execute(MockTerminal.create(), new OptionParser().parse(), env, null);

        KeyStoreWrapper nodeKeystore = KeyStoreWrapper.load(configDir.resolve("config"));
        nodeKeystore.decrypt(new char[0]); // the keystore is always bootstrapped with an empty password

        SecureString httpKeystorePassword = nodeKeystore.getString("xpack.security.http.ssl.keystore.secure_password");
        SecureString transportKeystorePassword = nodeKeystore.getString("xpack.security.transport.ssl.keystore.secure_password");

        final Settings newSettings = Settings.builder().loadFromPath(env.configDir().resolve("elasticsearch.yml")).build();
        final String httpKeystorePath = newSettings.get("xpack.security.http.ssl.keystore.path");
        final String transportKeystorePath = newSettings.get("xpack.security.transport.ssl.keystore.path");

        // the container type follows xpack.security.fips_mode.enabled, and the configured path must carry the matching extension
        final KeyStoreFormat expectedFormat = XPackSettings.FIPS_MODE_ENABLED.get(settings) ? KeyStoreFormat.BCFKS : KeyStoreFormat.PKCS12;
        assertThat(httpKeystorePath, endsWith(expectedFormat.extension()));
        assertThat(transportKeystorePath, endsWith(expectedFormat.extension()));

        KeyStore httpKeystore = expectedFormat.readKeyStore(
            configDir.resolve("config").resolve(httpKeystorePath),
            httpKeystorePassword.getChars()
        );
        assertThat(httpKeystore.getType(), equalTo(expectedFormat.type()));

        KeyStore transportKeystore = expectedFormat.readKeyStore(
            configDir.resolve("config").resolve(transportKeystorePath),
            transportKeystorePassword.getChars()
        );
        assertThat(transportKeystore.getType(), equalTo(expectedFormat.type()));

        X509Certificate httpCertificate = (X509Certificate) httpKeystore.getCertificate("http");
        X509Certificate transportCertificate = (X509Certificate) transportKeystore.getCertificate("transport");

        // node enrollment finds each layer's CA through the key entry's chain, so the CA must be stored in the chain and not only
        // as a separate trusted entry (PKCS#12 would reconstruct the chain on load, other keystore types such as BCFKS do not)
        assertCaInKeyEntryChain(httpKeystore, "http", "http_ca");
        assertCaInKeyEntryChain(transportKeystore, "transport", "transport_ca");

        return new Tuple<>(httpCertificate, transportCertificate);
    }

    private static void assertCaInKeyEntryChain(KeyStore keyStore, String keyAlias, String caAlias) throws Exception {
        final Certificate[] chain = keyStore.getCertificateChain(keyAlias);
        assertThat(chain.length, equalTo(2));
        final X509Certificate caCertificate = (X509Certificate) chain[1];
        assertThat(caCertificate.getBasicConstraints(), not(equalTo(-1)));
        assertThat(caCertificate, equalTo(keyStore.getCertificate(caAlias)));
    }

    private void deleteDirectory(Path directory) throws IOException {
        IOUtils.rm(directory);
    }
}
