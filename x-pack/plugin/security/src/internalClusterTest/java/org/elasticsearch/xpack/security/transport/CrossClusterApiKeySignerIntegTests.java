/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.transport;

import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.ssl.PemKeyConfig;
import org.elasticsearch.test.SecurityIntegTestCase;

import static org.elasticsearch.xpack.security.transport.CrossClusterApiKeySignerSettings.SIGNING_CERT_PATH;
import static org.elasticsearch.xpack.security.transport.CrossClusterApiKeySignerSettings.SIGNING_KEYSTORE_ALIAS;
import static org.elasticsearch.xpack.security.transport.CrossClusterApiKeySignerSettings.SIGNING_KEYSTORE_PATH;
import static org.elasticsearch.xpack.security.transport.CrossClusterApiKeySignerSettings.SIGNING_KEYSTORE_SECURE_PASSWORD;
import static org.elasticsearch.xpack.security.transport.CrossClusterApiKeySignerSettings.SIGNING_KEYSTORE_TYPE;
import static org.elasticsearch.xpack.security.transport.CrossClusterApiKeySignerSettings.SIGNING_KEY_PATH;
import static org.hamcrest.Matchers.equalToIgnoringCase;

public class CrossClusterApiKeySignerIntegTests extends SecurityIntegTestCase {

    private static final String DYNAMIC_TEST_CLUSTER_ALIAS = "dynamic_test_cluster";
    private static final String STATIC_TEST_CLUSTER_ALIAS = "static_test_cluster";

    public void testSignWithPemKeyConfig() {
        final CrossClusterApiKeySigner signer = internalCluster().getInstance(
            CrossClusterApiKeySigner.class,
            internalCluster().getRandomNodeName()
        );
        final String[] testHeaders = randomArray(5, String[]::new, () -> randomAlphanumericOfLength(randomInt(20)));

        X509CertificateSignature signature = signer.sign(STATIC_TEST_CLUSTER_ALIAS, testHeaders);
        signature.certificate().getPublicKey();

        var keyConfig = new PemKeyConfig(
            "signing_rsa.crt",
            "signing_rsa.key",
            new char[0],
            getDataPath("/org/elasticsearch/xpack/security/signature/signing_rsa.crt").getParent()
        );

        assertThat(signature.algorithm(), equalToIgnoringCase(keyConfig.getKeys().getFirst().v2().getSigAlgName()));
        assertEquals(signature.certificate(), keyConfig.getKeys().getFirst().v2());
    }

    public void testSignUnknownClusterAlias() {
        final CrossClusterApiKeySigner signer = internalCluster().getInstance(
            CrossClusterApiKeySigner.class,
            internalCluster().getRandomNodeName()
        );
        final String[] testHeaders = randomArray(5, String[]::new, () -> randomAlphanumericOfLength(randomInt(20)));

        X509CertificateSignature signature = signer.sign("unknowncluster", testHeaders);
        assertNull(signature);
    }

    public void testSeveralKeyStoreAliases() {
        final CrossClusterApiKeySigner signer = internalCluster().getInstance(
            CrossClusterApiKeySigner.class,
            internalCluster().getRandomNodeName()
        );

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
                X509CertificateSignature signature = signer.sign(DYNAMIC_TEST_CLUSTER_ALIAS, "test", "test");
                assertNull(signature);
            }

            // Add an alias from the keystore
            updateClusterSettings(
                Settings.builder()
                    .put(SIGNING_KEYSTORE_ALIAS.getConcreteSettingForNamespace(DYNAMIC_TEST_CLUSTER_ALIAS).getKey(), "wholelottakey")
            );
            {
                X509CertificateSignature signature = signer.sign(DYNAMIC_TEST_CLUSTER_ALIAS, "test", "test");
                assertNotNull(signature);
            }

            // Add an alias not in the keystore, settings should silently fail to apply
            updateClusterSettings(
                Settings.builder()
                    .put(SIGNING_KEYSTORE_ALIAS.getConcreteSettingForNamespace(DYNAMIC_TEST_CLUSTER_ALIAS).getKey(), "idonotexist")
            );
            {
                X509CertificateSignature signature = signer.sign(DYNAMIC_TEST_CLUSTER_ALIAS, "test", "test");
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
