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
import org.elasticsearch.common.ssl.PemKeyConfig;
import org.elasticsearch.test.SecurityIntegTestCase;

import javax.net.ssl.KeyManagerFactory;
import java.security.cert.X509Certificate;

import static org.elasticsearch.xpack.security.transport.CrossClusterApiKeySigningSettings.SIGNING_CERTIFICATE_AUTHORITIES;
import static org.elasticsearch.xpack.security.transport.CrossClusterApiKeySigningSettings.SIGNING_CERT_PATH;
import static org.elasticsearch.xpack.security.transport.CrossClusterApiKeySigningSettings.SIGNING_KEYSTORE_ALIAS;
import static org.elasticsearch.xpack.security.transport.CrossClusterApiKeySigningSettings.SIGNING_KEYSTORE_PATH;
import static org.elasticsearch.xpack.security.transport.CrossClusterApiKeySigningSettings.SIGNING_KEYSTORE_SECURE_PASSWORD;
import static org.elasticsearch.xpack.security.transport.CrossClusterApiKeySigningSettings.SIGNING_KEYSTORE_TYPE;
import static org.elasticsearch.xpack.security.transport.CrossClusterApiKeySigningSettings.SIGNING_KEY_PATH;
import static org.hamcrest.Matchers.equalToIgnoringCase;
import static org.mockito.Mockito.mock;

public class CrossClusterApiKeySignatureManagerIntegTests extends SecurityIntegTestCase {

    private static final String DYNAMIC_TEST_CLUSTER_ALIAS = "dynamic_test_cluster";
    private static final String STATIC_TEST_CLUSTER_ALIAS = "static_test_cluster";

    public void testSignAndVerifyWithPemKeyConfig() {
        final CrossClusterApiKeySignatureManager manager = internalCluster().getInstance(
            CrossClusterApiKeySignatureManager.class,
            internalCluster().getRandomNodeName()
        );
        final String[] testHeaders = randomArray(5, String[]::new, () -> randomAlphanumericOfLength(randomInt(20)));

        X509CertificateSignature signature = manager.signerForClusterAlias(STATIC_TEST_CLUSTER_ALIAS).sign(testHeaders);
        var keyConfig = new PemKeyConfig(
            "signing_rsa.crt",
            "signing_rsa.key",
            new char[0],
            getDataPath("/org/elasticsearch/xpack/security/signature/signing_rsa.crt").getParent()
        );

        var verifier = manager.verifierForClusterAlias(STATIC_TEST_CLUSTER_ALIAS);

        assertThat(signature.algorithm(), equalToIgnoringCase(keyConfig.getKeys().getFirst().v2().getSigAlgName()));
        assertEquals(signature.certificates()[0], keyConfig.getKeys().getFirst().v2());
        assertTrue(verifier.verify(signature, testHeaders));
    }

    public void testSignAndVerifyUnknownClusterAlias() {
        final CrossClusterApiKeySignatureManager manager = internalCluster().getInstance(
            CrossClusterApiKeySignatureManager.class,
            internalCluster().getRandomNodeName()
        );
        final String[] testHeaders = randomArray(5, String[]::new, () -> randomAlphanumericOfLength(randomInt(20)));
        X509CertificateSignature signature = manager.signerForClusterAlias("unknowncluster").sign(testHeaders);
        assertNull(signature);

        var signatureMock = new X509CertificateSignature(
            new X509Certificate[] { mock(X509Certificate.class) },
            KeyManagerFactory.getDefaultAlgorithm(),
            new BytesArray("test")
        );

        var exception = assertThrows(
            IllegalStateException.class,
            () -> manager.verifierForClusterAlias("unknowncluster").verify(signatureMock, testHeaders)
        );
        assertThat(exception.getMessage(), equalToIgnoringCase("No trust manager found for [unknowncluster]"));
    }

    public void testSeveralKeyStoreAliases() {
        final CrossClusterApiKeySignatureManager manager = internalCluster().getInstance(
            CrossClusterApiKeySignatureManager.class,
            internalCluster().getRandomNodeName()
        );
        var signer = manager.signerForClusterAlias(DYNAMIC_TEST_CLUSTER_ALIAS);

        try {
            // Create a new config without an alias. Since there are several aliases in the keystore, no signature should be generated
            updateClusterSettings(
                Settings.builder()
                    .put(
                        SIGNING_KEYSTORE_TYPE.getConcreteSettingForNamespace(DYNAMIC_TEST_CLUSTER_ALIAS).getKey(),
                        inFipsJvm() ? "BCFKS" : "PKCS12"
                    )
                    .put(
                        SIGNING_KEYSTORE_PATH.getConcreteSettingForNamespace(DYNAMIC_TEST_CLUSTER_ALIAS).getKey(),
                        getDataPath("/org/elasticsearch/xpack/security/signature/signing." + (inFipsJvm() ? "bcfks" : "jks"))
                    )
            );

            {
                X509CertificateSignature signature = signer.sign("test", "test");
                assertNull(signature);
            }

            // Add an alias from the keystore
            updateClusterSettings(
                Settings.builder()
                    .put(SIGNING_KEYSTORE_ALIAS.getConcreteSettingForNamespace(DYNAMIC_TEST_CLUSTER_ALIAS).getKey(), "wholelottakey")
            );
            {
                X509CertificateSignature signature = signer.sign("test", "test");
                assertNotNull(signature);
            }

            // Add an alias not in the keystore, settings should silently fail to apply
            updateClusterSettings(
                Settings.builder()
                    .put(SIGNING_KEYSTORE_ALIAS.getConcreteSettingForNamespace(DYNAMIC_TEST_CLUSTER_ALIAS).getKey(), "idonotexist")
            );
            {
                X509CertificateSignature signature = signer.sign("test", "test");
                assertNotNull(signature);
            }
        } finally {
            updateClusterSettings(
                Settings.builder()
                    .putNull(SIGNING_KEYSTORE_PATH.getConcreteSettingForNamespace(DYNAMIC_TEST_CLUSTER_ALIAS).getKey())
                    .putNull(SIGNING_KEYSTORE_ALIAS.getConcreteSettingForNamespace(DYNAMIC_TEST_CLUSTER_ALIAS).getKey())
                    .putNull(SIGNING_KEYSTORE_TYPE.getConcreteSettingForNamespace(DYNAMIC_TEST_CLUSTER_ALIAS).getKey())
                    .setSecureSettings(new MockSecureSettings())
            );
        }
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        var builder = Settings.builder();
        MockSecureSettings secureSettings = (MockSecureSettings) builder.put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(
                SIGNING_CERTIFICATE_AUTHORITIES.getConcreteSettingForNamespace(STATIC_TEST_CLUSTER_ALIAS).getKey(),
                getDataPath("/org" + "/elasticsearch/xpack/security/signature/root.crt")
            )
            .put(
                SIGNING_CERT_PATH.getConcreteSettingForNamespace(STATIC_TEST_CLUSTER_ALIAS).getKey(),
                getDataPath("/org/elasticsearch/xpack/security/signature/signing_rsa.crt")
            )
            .put(
                SIGNING_KEY_PATH.getConcreteSettingForNamespace(STATIC_TEST_CLUSTER_ALIAS).getKey(),
                getDataPath("/org/elasticsearch/xpack/security/signature/signing_rsa.key")
            )
            .getSecureSettings();
        secureSettings.setString(
            SIGNING_KEYSTORE_SECURE_PASSWORD.getConcreteSettingForNamespace(DYNAMIC_TEST_CLUSTER_ALIAS).getKey(),
            "secretpassword"
        );
        return builder.build();
    }
}
