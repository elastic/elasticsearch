/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.transport;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;

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

    public void testSignAndVerifyPKCS12orBCFKS() {
        var builder = Settings.builder()
            .put("cluster.remote.my_remote.signing.keystore.alias", "wholelottakey")
            .put("path.home", createTempDir())
            .put(Node.NODE_NAME_SETTING.getKey(), randomAlphaOfLengthBetween(3, 8));

        addStorePathToBuilder("my_remote", "signing", true, "secretpassword", "secretpassword", builder);
        addStorePathToBuilder("my_remote", "truststore", false, "changeit", "secretpassword", builder);

        var manager = new CrossClusterApiKeySignatureManager(TestEnvironment.newEnvironment(builder.build()));
        var signature = manager.signerForClusterAlias("my_remote").sign("a_header");
        assertTrue(manager.verifierForClusterAlias("my_remote").verify(signature, "a_header"));
    }

    public void testSignAndVerifyDifferentPayloadFailsPKCS12orBCFKS() {
        var builder = Settings.builder()
            .put("cluster.remote.my_remote.signing.keystore.alias", "wholelottakey")
            .put("path.home", createTempDir())
            .put(Node.NODE_NAME_SETTING.getKey(), randomAlphaOfLengthBetween(3, 8));

        addStorePathToBuilder("my_remote", "signing", true, "secretpassword", "secretpassword", builder);
        addStorePathToBuilder("my_remote", "truststore", false, "changeit", "secretpassword", builder);

        var manager = new CrossClusterApiKeySignatureManager(TestEnvironment.newEnvironment(builder.build()));
        var signature = manager.signerForClusterAlias("my_remote").sign("a_header");
        assertFalse(manager.verifierForClusterAlias("my_remote").verify(signature, "another_header"));
    }

    public void testSignAndVerifyRSAorEC() {
        var builder = Settings.builder()
            .put("path.home", createTempDir())
            .put(Node.NODE_NAME_SETTING.getKey(), randomAlphaOfLengthBetween(3, 8));

        addCertSigningTestCerts(builder);

        var manager = new CrossClusterApiKeySignatureManager(TestEnvironment.newEnvironment(builder.build()));
        var signature = manager.signerForClusterAlias("my_remote").sign("a_header");
        assertTrue(manager.verifierForClusterAlias("my_remote").verify(signature, "a_header"));
    }

    public void testSignAndVerifyDifferentPayloadFailsRSAorEC() {
        var builder = Settings.builder()
            .put("path.home", createTempDir())
            .put(Node.NODE_NAME_SETTING.getKey(), randomAlphaOfLengthBetween(3, 8));

        addCertSigningTestCerts(builder);

        var manager = new CrossClusterApiKeySignatureManager(TestEnvironment.newEnvironment(builder.build()));
        var signature = manager.signerForClusterAlias("my_remote").sign("a_header");
        assertFalse(manager.verifierForClusterAlias("my_remote").verify(signature, "another_header"));
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
            .put("cluster.remote.my_remote.signing.truststore.path", "not_a_valid_path")
            .put("path.home", createTempDir())
            .put(Node.NODE_NAME_SETTING.getKey(), randomAlphaOfLengthBetween(3, 8));

        var exception = assertThrows(
            IllegalStateException.class,
            () -> new CrossClusterApiKeySignatureManager(TestEnvironment.newEnvironment(builder.build()))
        );
        assertThat(exception.getMessage(), equalTo("Failed to load signing config for cluster [my_remote]"));
    }

    public void testLoadSeveralAliasesWithoutAliasSettingKeystore() {
        var builder = Settings.builder()
            .put("path.home", createTempDir())
            .put(Node.NODE_NAME_SETTING.getKey(), randomAlphaOfLengthBetween(3, 8));

        addStorePathToBuilder("my_remote", "signing", true, "secretpassword", "secretpassword", builder);

        var exception = assertThrows(
            IllegalStateException.class,
            () -> new CrossClusterApiKeySignatureManager(TestEnvironment.newEnvironment(builder.build()))
        );
        assertThat(exception.getMessage(), equalTo("Failed to load signing config for cluster [my_remote]"));
    }

    public void testSignAndVerifyIntermediateCertInChain() throws Exception {
        var ca = getDataPath("/org/elasticsearch/xpack/security/signature/root.crt");
        var builder = settingsBuilder.put("cluster.remote.my_remote.signing.certificate_authorities", ca);

        addStorePathToBuilder("my_remote", "signing_with_intermediate", true, "password123password", "password123password", builder);

        var environment = TestEnvironment.newEnvironment(builder.build());
        var manager = new CrossClusterApiKeySignatureManager(environment);
        var signer = manager.signerForClusterAlias("my_remote");
        var verifier = manager.verifierForClusterAlias("my_remote");

        var signature = signer.sign("test");
        assertThat(signature.certificates(), arrayWithSize(2));
        assertTrue(verifier.verify(signature, "test"));
    }

    public void testSignAndVerifyFailsIntermediateCertMissing() {
        var ca = getDataPath("/org/elasticsearch/xpack/security/signature/root.crt");
        var builder = settingsBuilder.put("cluster.remote.my_remote.signing.certificate_authorities", ca);

        addStorePathToBuilder("my_remote", "signing_no_intermediate", true, "password123password", "password123password", builder);

        var environment = TestEnvironment.newEnvironment(builder.build());
        var manager = new CrossClusterApiKeySignatureManager(environment);
        var signer = manager.signerForClusterAlias("my_remote");
        var verifier = manager.verifierForClusterAlias("my_remote");

        var signature = signer.sign("test");
        assertThat(signature.certificates(), arrayWithSize(1));
        var exception = assertThrows(ElasticsearchSecurityException.class, () -> verifier.verify(signature, "test"));
        assertThat(exception.getMessage(), containsString("Failed to verify signature for [my_remote]"));
    }

    private void addStorePathToBuilder(
        String remoteCluster,
        String storeName,
        boolean isKeyStore,
        String password,
        String passwordFips,
        Settings.Builder builder
    ) {
        String storeType = inFipsJvm() ? "BCFKS" : "PKCS12";
        String extension = inFipsJvm() ? ".bcfks" : ".jks";
        String storeKind = isKeyStore ? "keystore" : "truststore";
        String keystorePassword = inFipsJvm() ? passwordFips : password;

        if (builder.getSecureSettings() == null) {
            builder.setSecureSettings(new MockSecureSettings());
        }
        MockSecureSettings secureSettings = (MockSecureSettings) builder.getSecureSettings();

        secureSettings.setString("cluster.remote." + remoteCluster + ".signing." + storeKind + ".secure_password", keystorePassword);

        builder.put("cluster.remote." + remoteCluster + ".signing." + storeKind + ".type", storeType)
            .put(
                "cluster.remote." + remoteCluster + ".signing." + storeKind + ".path",
                getDataPath("/org/elasticsearch/xpack/security/signature/" + storeName + extension)
            );
    }

    private void addCertSigningTestCerts(Settings.Builder builder) {
        String caPath;
        String certPath;
        String keyPath;

        if (inFipsJvm() == false) {
            MockSecureSettings secureSettings = new MockSecureSettings();
            secureSettings.setString("cluster.remote.my_remote.signing.secure_key_passphrase", "marshall");
            builder.setSecureSettings(secureSettings);

            caPath = "/org/elasticsearch/xpack/security/signature/signing_ec.crt";
            certPath = "/org/elasticsearch/xpack/security/signature/signing_ec.crt";
            keyPath = "/org/elasticsearch/xpack/security/signature/signing_ec.key";
        } else {
            caPath = "/org/elasticsearch/xpack/security/signature/root.crt";
            certPath = "/org/elasticsearch/xpack/security/signature/signing_rsa.crt";
            keyPath = "/org/elasticsearch/xpack/security/signature/signing_rsa.key";
        }

        builder.put("cluster.remote.my_remote.signing.certificate_authorities", getDataPath(caPath))
            .put("cluster.remote.my_remote.signing.certificate", getDataPath(certPath))
            .put("cluster.remote.my_remote.signing.key", getDataPath(keyPath));
    }

    @After
    public void tearDownThreadPool() {
        terminate(threadPool);
    }

}
