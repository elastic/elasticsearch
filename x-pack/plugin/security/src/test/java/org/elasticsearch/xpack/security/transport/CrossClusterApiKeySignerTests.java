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

import static org.hamcrest.Matchers.equalTo;

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
        var exception = assertThrows(
            IllegalStateException.class,
            () -> new CrossClusterApiKeySigner(TestEnvironment.newEnvironment(builder.build()))
        );
        assertThat(exception.getMessage(), equalTo("Failed to load signing config for cluster [my_remote]"));

    }

    public void testLoadSeveralAliasesWithoutAliasSettingKeystore() {
        var builder = Settings.builder()
            .put("path.home", createTempDir())
            .put(Node.NODE_NAME_SETTING.getKey(), randomAlphaOfLengthBetween(3, 8));

        addKeyStorePathToBuilder("my_remote", builder);
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("cluster.remote.my_remote.signing.keystore.secure_password", "secretpassword");
        builder.setSecureSettings(secureSettings);
        var exception = assertThrows(
            IllegalStateException.class,
            () -> new CrossClusterApiKeySigner(TestEnvironment.newEnvironment(builder.build()))
        );
        assertThat(exception.getMessage(), equalTo("Failed to load signing config for cluster [my_remote]"));
    }

    private void addKeyStorePathToBuilder(String remoteCluster, Settings.Builder builder) {
        builder.put("cluster.remote." + remoteCluster + ".signing.keystore.type", inFipsJvm() ? "BCFKS" : "PKCS12")
            .put(
                "cluster.remote." + remoteCluster + ".signing.keystore.path",
                getDataPath("/org/elasticsearch/xpack/security/signature/signing." + (inFipsJvm() ? "bcfks" : "jks"))
            );
    }
}
