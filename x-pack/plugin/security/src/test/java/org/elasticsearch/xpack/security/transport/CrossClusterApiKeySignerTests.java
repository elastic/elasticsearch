/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.transport;

import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.ESTestCase;

import java.util.Map;
import java.util.Set;

public class CrossClusterApiKeySignerTests extends ESTestCase {

    public void testLoadKeystore() {
        var builder = Settings.builder()
            .put("cluster.remote.my_remote.signing.keystore.alias", "wholelottakey")
            .put("path.home", createTempDir())
            .put(Node.NODE_NAME_SETTING.getKey(), randomAlphaOfLengthBetween(3, 8));
        addKeyStorePathToBuilder("my_remote", builder);
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("cluster.remote.my_remote.signing.keystore.secure_password", "secretpassword");
        builder.setSecureSettings(secureSettings);
        var signer = new CrossClusterApiKeySigner(TestEnvironment.newEnvironment(builder.build()));

        assertNotNull(signer.sign("my_remote", "a_header"));
    }

    public void testLoadKeystoreMissingFile() {
        var builder = Settings.builder()
            .put("cluster.remote.my_remote.signing.keystore.alias", "wholelottakey")
            .put("cluster.remote.my_remote.signing.keystore.path", "not_a_valid_path")
            .put("path.home", createTempDir())
            .put(Node.NODE_NAME_SETTING.getKey(), randomAlphaOfLengthBetween(3, 8));

        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("cluster.remote.my_remote.signing.keystore.secure_password", "secretpassword");
        builder.setSecureSettings(secureSettings);
        var signer = new CrossClusterApiKeySigner(TestEnvironment.newEnvironment(builder.build()));

        assertNull(signer.sign("my_remote", "a_header"));
    }

    public void testLoadSeveralAliasesWithoutAliasSettingKeystore() {
        var builder = Settings.builder()
            .put("path.home", createTempDir())
            .put(Node.NODE_NAME_SETTING.getKey(), randomAlphaOfLengthBetween(3, 8));

        addKeyStorePathToBuilder("my_remote", builder);
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("cluster.remote.my_remote.signing.keystore.secure_password", "secretpassword");
        builder.setSecureSettings(secureSettings);
        var signer = new CrossClusterApiKeySigner(TestEnvironment.newEnvironment(builder.build()));

        assertNull(signer.sign("my_remote", "a_header"));
    }

    public void testGetDependentFilesToClusterAliases() {
        var builder = Settings.builder()
            .put("cluster.remote.my_remote1.signing.keystore.alias", "wholelottakey")
            .put("cluster.remote.my_remote2.signing.keystore.alias", "wholelottakey")
            .put(
                "cluster.remote.my_remote3.signing.certificate",
                getDataPath("/org/elasticsearch/xpack/security/signature/signing_rsa.crt")
            )
            .put("cluster.remote.my_remote3.signing.key", getDataPath("/org/elasticsearch/xpack/security/signature/signing_rsa.key"))
            .put(
                "cluster.remote.my_remote4.signing.certificate",
                getDataPath("/org/elasticsearch/xpack/security/signature/signing_rsa.crt")
            )
            .put("cluster.remote.my_remote4.signing.key", getDataPath("/org/elasticsearch/xpack/security/signature/signing_rsa.key"))
            .put("path.home", createTempDir())
            .put(Node.NODE_NAME_SETTING.getKey(), randomAlphaOfLengthBetween(3, 8));
        addKeyStorePathToBuilder("my_remote1", builder);
        addKeyStorePathToBuilder("my_remote2", builder);
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("cluster.remote.my_remote1.signing.keystore.secure_password", "secretpassword");
        secureSettings.setString("cluster.remote.my_remote2.signing.keystore.secure_password", "secretpassword");
        builder.setSecureSettings(secureSettings);
        var signer = new CrossClusterApiKeySigner(TestEnvironment.newEnvironment(builder.build()));
        assertNotNull(signer.sign(randomFrom("my_remote1", "my_remote2", "my_remote3", "my_remote4"), "a_header"));

        assertEquals(
            Map.of(
                getDataPath("/org/elasticsearch/xpack/security/signature/signing." + (inFipsJvm() ? "bcfks" : "jks")),
                Set.of("my_remote1", "my_remote2"),
                getDataPath("/org/elasticsearch/xpack/security/signature/signing_rsa.crt"),
                Set.of("my_remote3", "my_remote4"),
                getDataPath("/org/elasticsearch/xpack/security/signature/signing_rsa.key"),
                Set.of("my_remote3", "my_remote4")
            ),
            signer.getDependentFilesToClusterAliases()
        );
    }

    private void addKeyStorePathToBuilder(String remoteCluster, Settings.Builder builder) {
        builder.put("cluster.remote." + remoteCluster + ".signing.keystore.type", inFipsJvm() ? "BCFKS" : "PKCS12")
            .put(
                "cluster.remote." + remoteCluster + ".signing.keystore.path",
                getDataPath("/org/elasticsearch/xpack/security/signature/signing." + (inFipsJvm() ? "bcfks" : "jks"))
            );
    }
}
