/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.transport;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;

import java.security.GeneralSecurityException;
import java.security.cert.CertificateException;

import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class CrossClusterApiKeySignatureManagerTests extends ESTestCase {
    private ThreadPool threadPool;
    private Settings.Builder settingsBuilder;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getTestName());
        settingsBuilder = Settings.builder()
            .put("path.home", createTempDir())
            .put(Node.NODE_NAME_SETTING.getKey(), randomAlphaOfLengthBetween(3, 8));
    }

    public void testSignAndVerifyPKCS12orBCFKS() throws GeneralSecurityException {
        var builder = Settings.builder()
            .put("cluster.remote.my_remote.signing.keystore.alias", "wholelottakey")
            .put("path.home", createTempDir())
            .put(Node.NODE_NAME_SETTING.getKey(), randomAlphaOfLengthBetween(3, 8));

        addStorePathToBuilder("my_remote", "signing", "secretpassword", "secretpassword", builder);
        addStorePathToBuilder("truststore", "changeit", "secretpassword", builder);

        var manager = new CrossClusterApiKeySignatureManager(TestEnvironment.newEnvironment(builder.build()));
        var signature = manager.signerForClusterAlias("my_remote").sign("a_header");
        assertTrue(manager.verifier().verify(signature, "a_header"));
    }

    public void testSignAndVerifyDifferentPayloadFailsPKCS12orBCFKS() throws GeneralSecurityException {
        var builder = Settings.builder()
            .put("cluster.remote.my_remote.signing.keystore.alias", "wholelottakey")
            .put("path.home", createTempDir())
            .put(Node.NODE_NAME_SETTING.getKey(), randomAlphaOfLengthBetween(3, 8));

        addStorePathToBuilder("my_remote", "signing", "secretpassword", "secretpassword", builder);
        addStorePathToBuilder("truststore", "changeit", "secretpassword", builder);

        var manager = new CrossClusterApiKeySignatureManager(TestEnvironment.newEnvironment(builder.build()));
        var signature = manager.signerForClusterAlias("my_remote").sign("a_header");
        assertFalse(manager.verifier().verify(signature, "another_header"));
    }

    public void testSignAndVerifyRSAorEC() throws GeneralSecurityException {
        var builder = Settings.builder()
            .put("path.home", createTempDir())
            .put(Node.NODE_NAME_SETTING.getKey(), randomAlphaOfLengthBetween(3, 8));

        addCertSigningTestCerts("my_remote", builder);

        var manager = new CrossClusterApiKeySignatureManager(TestEnvironment.newEnvironment(builder.build()));
        var signature = manager.signerForClusterAlias("my_remote").sign("a_header");
        assertTrue(manager.verifier().verify(signature, "a_header"));
    }

    public void testSignAndVerifyDifferentPayloadFailsRSAorEC() throws GeneralSecurityException {
        var builder = Settings.builder()
            .put("path.home", createTempDir())
            .put(Node.NODE_NAME_SETTING.getKey(), randomAlphaOfLengthBetween(3, 8));

        addCertSigningTestCerts("my_remote", builder);

        var manager = new CrossClusterApiKeySignatureManager(TestEnvironment.newEnvironment(builder.build()));
        var signature = manager.signerForClusterAlias("my_remote").sign("a_header");
        assertFalse(manager.verifier().verify(signature, "another_header"));
    }

    public void testSignAndVerifyWrongKeyRSAorEC() throws GeneralSecurityException {
        var builder = Settings.builder()
            .put("cluster.remote.my_remote2.signing.keystore.alias", "ainttalkinboutkeys")
            .put("path.home", createTempDir())
            .put(Node.NODE_NAME_SETTING.getKey(), randomAlphaOfLengthBetween(3, 8));

        addStorePathToBuilder("my_remote2", "signing", "secretpassword", "secretpassword", builder);
        addCertSigningTestCerts("my_remote1", builder, true);

        var manager = new CrossClusterApiKeySignatureManager(TestEnvironment.newEnvironment(builder.build()));
        var verifier = manager.verifier();

        var signature1 = manager.signerForClusterAlias("my_remote1").sign("a_header");
        assertTrue(verifier.verify(signature1, "a_header"));
        // Signature generated with different key and cert chain
        var signature2 = manager.signerForClusterAlias("my_remote2").sign("a_header");
        // Replace only key
        assertFalse(
            verifier.verify(
                new X509CertificateSignature(signature1.certificates(), signature1.algorithm(), signature2.signature()),
                "a_header"
            )
        );
    }

    public void testSignAndVerifyManipulatedSignatureStringRSAorEC() throws GeneralSecurityException {
        var builder = Settings.builder()
            .put("path.home", createTempDir())
            .put(Node.NODE_NAME_SETTING.getKey(), randomAlphaOfLengthBetween(3, 8));

        addCertSigningTestCerts("my_remote", builder);

        var manager = new CrossClusterApiKeySignatureManager(TestEnvironment.newEnvironment(builder.build()));
        var signature = manager.signerForClusterAlias("my_remote").sign("a_header");
        var bytes = signature.signature().array();
        bytes[bytes.length - 1] ^= 0x01;
        var manipulatedSignature = new X509CertificateSignature(signature.certificates(), signature.algorithm(), new BytesArray(bytes));

        assertFalse(manager.verifier().verify(manipulatedSignature, "a_header"));
    }

    public void testLoadKeystoreMissingFile() {
        var builder = Settings.builder()
            .put("cluster.remote.my_remote.signing.keystore.path", "not_a_valid_path")
            .put("path.home", createTempDir())
            .put(Node.NODE_NAME_SETTING.getKey(), randomAlphaOfLengthBetween(3, 8));

        var exception = assertThrows(
            IllegalStateException.class,
            () -> new CrossClusterApiKeySignatureManager(TestEnvironment.newEnvironment(builder.build()))
        );
        assertThat(exception.getMessage(), equalTo("Failed to load signing config for cluster [my_remote]"));
    }

    public void testLoadTruststoreMissingFile() {
        var builder = Settings.builder()
            .put("cluster.remote.signing.truststore.path", "not_a_valid_path")
            .put("path.home", createTempDir())
            .put(Node.NODE_NAME_SETTING.getKey(), randomAlphaOfLengthBetween(3, 8));

        var exception = assertThrows(
            IllegalStateException.class,
            () -> new CrossClusterApiKeySignatureManager(TestEnvironment.newEnvironment(builder.build()))
        );
        assertThat(exception.getMessage(), equalTo("Failed to load trust config"));
    }

    public void testLoadSeveralAliasesWithoutAliasSettingKeystore() {
        var builder = Settings.builder()
            .put("path.home", createTempDir())
            .put(Node.NODE_NAME_SETTING.getKey(), randomAlphaOfLengthBetween(3, 8));

        addStorePathToBuilder("my_remote", "signing", "secretpassword", "secretpassword", builder);

        var exception = assertThrows(
            IllegalStateException.class,
            () -> new CrossClusterApiKeySignatureManager(TestEnvironment.newEnvironment(builder.build()))
        );
        assertThat(exception.getMessage(), equalTo("Failed to load signing config for cluster [my_remote]"));
    }

    public void testSignAndVerifyIntermediateCertInChain() throws Exception {
        var ca = getDataPath("/org/elasticsearch/xpack/security/signature/root.crt");
        var builder = settingsBuilder.put("cluster.remote.signing.certificate_authorities", ca);

        addStorePathToBuilder("my_remote", "signing_with_intermediate", "password123password", "password123password", builder);

        var environment = TestEnvironment.newEnvironment(builder.build());
        var manager = new CrossClusterApiKeySignatureManager(environment);
        var signer = manager.signerForClusterAlias("my_remote");
        var verifier = manager.verifier();

        var signature = signer.sign("test");
        assertThat(signature.certificates(), arrayWithSize(2));
        assertTrue(verifier.verify(signature, "test"));
    }

    public void testSignAndVerifyFailsIntermediateCertMissing() {
        var ca = getDataPath("/org/elasticsearch/xpack/security/signature/root.crt");
        var builder = settingsBuilder.put("cluster.remote.signing.certificate_authorities", ca);

        addStorePathToBuilder("my_remote", "signing_no_intermediate", "password123password", "password123password", builder);

        var environment = TestEnvironment.newEnvironment(builder.build());
        var manager = new CrossClusterApiKeySignatureManager(environment);
        var signer = manager.signerForClusterAlias("my_remote");
        var verifier = manager.verifier();

        var signature = signer.sign("test");
        assertThat(signature.certificates(), arrayWithSize(1));
        var exception = assertThrows(GeneralSecurityException.class, () -> verifier.verify(signature, "test"));
        assertThat(
            exception.getMessage(),
            containsString(
                inFipsJvm() ? "Unable to construct a valid chain" : "unable to find valid certification path to requested target"
            )
        );
    }

    public void testSignAndVerifyExpiredCertFails() {
        var builder = Settings.builder()
            .put("path.home", createTempDir())
            .put(Node.NODE_NAME_SETTING.getKey(), randomAlphaOfLengthBetween(3, 8));

        builder.put(
            "cluster.remote.signing.certificate_authorities",
            getDataPath("/org/elasticsearch/xpack/security/signature/expired_cert.crt")
        )
            .put(
                "cluster.remote.my_remote.signing.certificate",
                getDataPath("/org/elasticsearch/xpack/security/signature/expired_cert.crt")
            )
            .put("cluster.remote.my_remote.signing.key", getDataPath("/org/elasticsearch/xpack/security/signature/expired_key.key"));

        var manager = new CrossClusterApiKeySignatureManager(TestEnvironment.newEnvironment(builder.build()));
        var signature = manager.signerForClusterAlias("my_remote").sign("a_header");
        var verifier = manager.verifier();
        var exception = assertThrows(CertificateException.class, () -> verifier.verify(signature, "test"));
        assertThat(exception.getMessage(), containsString(inFipsJvm() ? "certificate expired on" : "NotAfter"));
    }

    private void addStorePathToBuilder(String storeName, String password, String passwordFips, Settings.Builder builder) {
        String storeType = inFipsJvm() ? "BCFKS" : "PKCS12";
        String extension = inFipsJvm() ? ".bcfks" : ".jks";
        String keystorePassword = inFipsJvm() ? passwordFips : password;

        if (builder.getSecureSettings() == null) {
            builder.setSecureSettings(new MockSecureSettings());
        }
        MockSecureSettings secureSettings = (MockSecureSettings) builder.getSecureSettings();

        secureSettings.setString("cluster.remote.signing.truststore.secure_password", keystorePassword);

        builder.put("cluster.remote.signing.truststore.type", storeType)
            .put(
                "cluster.remote.signing.truststore.path",
                getDataPath("/org/elasticsearch/xpack/security/signature/" + storeName + extension)
            );
    }

    private void addStorePathToBuilder(
        String remoteCluster,
        String storeName,
        String password,
        String passwordFips,
        Settings.Builder builder
    ) {
        String storeType = inFipsJvm() ? "BCFKS" : "PKCS12";
        String extension = inFipsJvm() ? ".bcfks" : ".jks";
        String keystorePassword = inFipsJvm() ? passwordFips : password;

        if (builder.getSecureSettings() == null) {
            builder.setSecureSettings(new MockSecureSettings());
        }
        MockSecureSettings secureSettings = (MockSecureSettings) builder.getSecureSettings();

        secureSettings.setString("cluster.remote." + remoteCluster + ".signing.keystore.secure_password", keystorePassword);

        builder.put("cluster.remote." + remoteCluster + ".signing.keystore.type", storeType)
            .put(
                "cluster.remote." + remoteCluster + ".signing.keystore.path",
                getDataPath("/org/elasticsearch/xpack/security/signature/" + storeName + extension)
            );
    }

    private void addCertSigningTestCerts(String remoteCluster, Settings.Builder builder) {
        addCertSigningTestCerts(remoteCluster, builder, false);
    }

    private void addCertSigningTestCerts(String remoteCluster, Settings.Builder builder, boolean forceRSA) {
        String caPath;
        String certPath;
        String keyPath;

        if (inFipsJvm() == false && forceRSA == false) {
            if (builder.getSecureSettings() == null) {
                builder.setSecureSettings(new MockSecureSettings());
            }
            MockSecureSettings secureSettings = (MockSecureSettings) builder.getSecureSettings();
            secureSettings.setString("cluster.remote." + remoteCluster + ".signing.secure_key_passphrase", "marshall");

            caPath = "/org/elasticsearch/xpack/security/signature/signing_ec.crt";
            certPath = "/org/elasticsearch/xpack/security/signature/signing_ec.crt";
            keyPath = "/org/elasticsearch/xpack/security/signature/signing_ec.key";
        } else {
            caPath = "/org/elasticsearch/xpack/security/signature/root.crt";
            certPath = "/org/elasticsearch/xpack/security/signature/signing_rsa.crt";
            keyPath = "/org/elasticsearch/xpack/security/signature/signing_rsa.key";
        }

        builder.put("cluster.remote.signing.certificate_authorities", getDataPath(caPath))
            .put("cluster.remote." + remoteCluster + ".signing.certificate", getDataPath(certPath))
            .put("cluster.remote." + remoteCluster + ".signing.key", getDataPath(keyPath));
    }

    @After
    public void tearDownThreadPool() {
        terminate(threadPool);
    }

}
