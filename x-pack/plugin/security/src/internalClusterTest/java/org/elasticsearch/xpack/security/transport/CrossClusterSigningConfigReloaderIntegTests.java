/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.transport;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.reload.NodesReloadSecureSettingsRequest;
import org.elasticsearch.action.admin.cluster.node.reload.NodesReloadSecureSettingsResponse;
import org.elasticsearch.action.admin.cluster.node.reload.TransportNodesReloadSecureSettingsAction;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.KeyStoreWrapper;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.SecurityIntegTestCase;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import javax.net.ssl.KeyManagerFactory;

import static org.elasticsearch.xpack.security.transport.CrossClusterApiKeySignerSettings.SIGNING_CERT_PATH;
import static org.elasticsearch.xpack.security.transport.CrossClusterApiKeySignerSettings.SIGNING_KEYSTORE_ALGORITHM;
import static org.elasticsearch.xpack.security.transport.CrossClusterApiKeySignerSettings.SIGNING_KEYSTORE_ALIAS;
import static org.elasticsearch.xpack.security.transport.CrossClusterApiKeySignerSettings.SIGNING_KEYSTORE_PATH;
import static org.elasticsearch.xpack.security.transport.CrossClusterApiKeySignerSettings.SIGNING_KEYSTORE_SECURE_PASSWORD;
import static org.elasticsearch.xpack.security.transport.CrossClusterApiKeySignerSettings.SIGNING_KEYSTORE_TYPE;
import static org.elasticsearch.xpack.security.transport.CrossClusterApiKeySignerSettings.SIGNING_KEY_PATH;
import static org.elasticsearch.xpack.security.transport.CrossClusterApiKeySignerSettings.SIGNING_KEY_SECURE_PASSPHRASE;
import static org.hamcrest.Matchers.equalTo;

public class CrossClusterSigningConfigReloaderIntegTests extends SecurityIntegTestCase {

    public void testAddAndRemoveClusterConfigsRuntime() throws Exception {
        addAndRemoveClusterConfigsRuntime(randomClusterAliases(), clusterAlias -> {
            updateClusterSettings(
                Settings.builder()
                    .put(
                        SIGNING_CERT_PATH.getConcreteSettingForNamespace(clusterAlias).getKey(),
                        getDataPath("/org/elasticsearch/xpack/security/signature/signing_rsa.crt")
                    )
                    .put(
                        SIGNING_KEY_PATH.getConcreteSettingForNamespace(clusterAlias).getKey(),
                        getDataPath("/org/elasticsearch/xpack/security/signature/signing_rsa.key")
                    )
            );
        }, clusterAlias -> {
            updateClusterSettings(
                Settings.builder()
                    .putNull(SIGNING_CERT_PATH.getConcreteSettingForNamespace(clusterAlias).getKey())
                    .putNull(SIGNING_KEY_PATH.getConcreteSettingForNamespace(clusterAlias).getKey())
            );
        });
    }

    public void testAddSecureSettingsConfigRuntime() throws Exception {
        addAndRemoveClusterConfigsRuntime(randomClusterAliases(), clusterAlias -> {
            writeSecureSettingsToKeyStoreAndReload(
                Map.of(
                    SIGNING_KEYSTORE_SECURE_PASSWORD.getConcreteSettingForNamespace(clusterAlias).getKey(),
                    "secretpassword".toCharArray()
                )
            );
            updateClusterSettings(
                Settings.builder()
                    .put(
                        SIGNING_KEYSTORE_ALGORITHM.getConcreteSettingForNamespace(clusterAlias).getKey(),
                        KeyManagerFactory.getDefaultAlgorithm()
                    )
                    .put(SIGNING_KEYSTORE_ALIAS.getConcreteSettingForNamespace(clusterAlias).getKey(), "wholelottakey")
                    .put(SIGNING_KEYSTORE_TYPE.getConcreteSettingForNamespace(clusterAlias).getKey(), inFipsJvm() ? "BCFKS" : "PKCS12")
                    .put(
                        SIGNING_KEYSTORE_PATH.getConcreteSettingForNamespace(clusterAlias).getKey(),
                        getDataPath("/org/elasticsearch/xpack/security/signature/signing." + (inFipsJvm() ? "bcfks" : "jks"))
                    )
            );
        }, clusterAlias -> {
            updateClusterSettings(
                Settings.builder()
                    .putNull(SIGNING_KEYSTORE_PATH.getConcreteSettingForNamespace(clusterAlias).getKey())
                    .putNull(SIGNING_KEYSTORE_TYPE.getConcreteSettingForNamespace(clusterAlias).getKey())
                    .putNull(SIGNING_KEYSTORE_ALIAS.getConcreteSettingForNamespace(clusterAlias).getKey())
                    .putNull(SIGNING_KEYSTORE_ALGORITHM.getConcreteSettingForNamespace(clusterAlias).getKey())
                    .setSecureSettings(new MockSecureSettings())
            );
            removeSecureSettingsFromKeyStoreAndReload(
                Set.of(SIGNING_KEYSTORE_SECURE_PASSWORD.getConcreteSettingForNamespace(clusterAlias).getKey())
            );
        });
    }

    public void testDependentKeyConfigFilesUpdated() throws Exception {
        assumeFalse("Test credentials uses key encryption not supported in Fips JVM", inFipsJvm());
        final CrossClusterApiKeySigner signer = internalCluster().getInstance(
            CrossClusterApiKeySigner.class,
            internalCluster().getRandomNodeName()
        );

        String testClusterAlias = "test_cluster";

        try {
            // Write passphrase for ec key to keystore
            writeSecureSettingsToKeyStoreAndReload(
                Map.of(SIGNING_KEY_SECURE_PASSPHRASE.getConcreteSettingForNamespace(testClusterAlias).getKey(), "marshall".toCharArray())
            );

            assertNull(signer.sign(testClusterAlias, "a_header"));
            Path tempDir = createTempDir();
            Path signingCert = tempDir.resolve("signing.crt");
            Files.copy(getDataPath("/org/elasticsearch/xpack/security/signature/signing_rsa.crt"), signingCert);
            Path signingKey = tempDir.resolve("signing.key");
            Files.copy(getDataPath("/org/elasticsearch/xpack/security/signature/signing_rsa.key"), signingKey);

            Path updatedSigningCert = tempDir.resolve("updated_signing.crt");
            Files.copy(getDataPath("/org/elasticsearch/xpack/security/signature/signing_ec.crt"), updatedSigningCert);
            Path updatedSigningKey = tempDir.resolve("updated_signing.key");
            Files.copy(getDataPath("/org/elasticsearch/xpack/security/signature/signing_ec.key"), updatedSigningKey);

            // Add the cluster
            updateClusterSettings(
                Settings.builder()
                    .put(SIGNING_CERT_PATH.getConcreteSettingForNamespace(testClusterAlias).getKey(), signingCert)
                    .put(SIGNING_KEY_PATH.getConcreteSettingForNamespace(testClusterAlias).getKey(), signingKey)
            );

            // Make sure a signature can be created
            var signatureBefore = signer.sign(testClusterAlias, "test", "test");
            assertNotNull(signatureBefore);

            Files.move(updatedSigningCert, signingCert, StandardCopyOption.REPLACE_EXISTING);
            Files.move(updatedSigningKey, signingKey, StandardCopyOption.REPLACE_EXISTING);

            assertBusy(() -> {
                var signatureAfter = signer.sign(testClusterAlias, "test", "test");
                assertNotNull(signatureAfter);
                assertNotEquals(signatureAfter, signatureBefore);
            });
        } finally {
            updateClusterSettings(
                Settings.builder()
                    .putNull(SIGNING_CERT_PATH.getConcreteSettingForNamespace(testClusterAlias).getKey())
                    .putNull(SIGNING_KEY_PATH.getConcreteSettingForNamespace(testClusterAlias).getKey())
                    .setSecureSettings(new MockSecureSettings())
            );
            removeSecureSettingsFromKeyStoreAndReload(
                Set.of(SIGNING_KEYSTORE_SECURE_PASSWORD.getConcreteSettingForNamespace(testClusterAlias).getKey())
            );
        }
    }

    public void testRemoveFileWithConfig() throws Exception {
        try {
            final CrossClusterApiKeySigner signer = internalCluster().getInstance(
                CrossClusterApiKeySigner.class,
                internalCluster().getRandomNodeName()
            );

            assertNull(signer.sign("test_cluster", "a_header"));
            Path tempDir = createTempDir();
            Path signingCert = tempDir.resolve("signing.crt");
            Files.copy(getDataPath("/org/elasticsearch/xpack/security/signature/signing_rsa.crt"), signingCert);
            Path signingKey = tempDir.resolve("signing.key");
            Files.copy(getDataPath("/org/elasticsearch/xpack/security/signature/signing_rsa.key"), signingKey);

            // Add the cluster
            updateClusterSettings(
                Settings.builder()
                    .put("cluster.remote.test_cluster.signing.certificate", signingCert)
                    .put("cluster.remote.test_cluster.signing.key", signingKey)
            );

            // Make sure a signature can be created
            var signatureBefore = signer.sign("test_cluster", "test", "test");
            assertNotNull(signatureBefore);

            // This should just fail the update, not remove any actual configs
            Files.delete(signingCert);
            Files.delete(signingKey);

            var signatureAfter = signer.sign("test_cluster", "test", "test");
            assertNotNull(signatureAfter);
            assertEquals(signatureAfter, signatureBefore);
        } finally {
            updateClusterSettings(
                Settings.builder()
                    .putNull("cluster.remote.test_cluster.signing.certificate")
                    .putNull("cluster.remote.test_cluster.signing.key")
                    .setSecureSettings(new MockSecureSettings())
            );
        }
    }

    public void testValidationFailsWhenUpdateWithInvalidPath() throws Exception {
        Path unknownFile = createTempDir().resolve("unknown_file");
        var exception = assertThrows(
            IllegalArgumentException.class,
            () -> updateClusterSettings(
                Settings.builder()
                    .put(SIGNING_CERT_PATH.getConcreteSettingForNamespace("test").getKey(), unknownFile)
                    .put(SIGNING_KEY_PATH.getConcreteSettingForNamespace("test").getKey(), unknownFile)
            )
        );
        assertThat(exception.getMessage(), equalTo("File [" + unknownFile + "] configured for remote cluster [test] does no exist"));
    }

    private void addAndRemoveClusterConfigsRuntime(
        Set<String> clusterAliases,
        Consumer<String> clusterCreator,
        Consumer<String> clusterRemover
    ) throws Exception {
        final CrossClusterApiKeySigner signer = internalCluster().getInstance(
            CrossClusterApiKeySigner.class,
            internalCluster().getRandomNodeName()
        );
        final String[] testHeaders = randomArray(5, String[]::new, () -> randomAlphanumericOfLength(randomInt(20)));

        try {
            for (var clusterAlias : clusterAliases) {
                // Try to create a signature for a remote cluster that doesn't exist
                assertNull(signer.sign(clusterAlias, testHeaders));
                clusterCreator.accept(clusterAlias);
                // Make sure a signature can be created
                assertNotNull(signer.sign(clusterAlias, testHeaders));
            }
            for (var clusterAlias : clusterAliases) {
                clusterRemover.accept(clusterAlias);
                // Make sure no signature was created
                assertBusy(() -> assertNull(signer.sign(clusterAlias, testHeaders)));
            }
        } finally {
            var builder = Settings.builder();
            for (var clusterAlias : clusterAliases) {
                CrossClusterApiKeySignerSettings.getDynamicSettings().forEach(setting -> {
                    builder.putNull(setting.getConcreteSettingForNamespace(clusterAlias).getKey());
                });
            }
            if (clusterAliases.isEmpty() == false) {
                updateClusterSettings(builder.setSecureSettings(new MockSecureSettings()));
            }
        }
    }

    private Set<String> randomClusterAliases() {
        return randomUnique(() -> randomAlphaOfLengthBetween(1, randomIntBetween(5, 20)), randomInt(5));
    }

    private void writeSecureSettingsToKeyStoreAndReload(Map<String, char[]> entries) {
        char[] keyStorePassword = randomAlphaOfLengthBetween(15, randomIntBetween(15, 20)).toCharArray();
        internalCluster().getInstances(Environment.class).forEach(environment -> {
            final KeyStoreWrapper keyStoreWrapper = KeyStoreWrapper.create();
            entries.forEach(keyStoreWrapper::setString);
            try {
                keyStoreWrapper.save(environment.configDir(), keyStorePassword, false);
                logger.info(keyStoreWrapper.toString());
            } catch (Exception e) {
                fail(e.getMessage());
            }
        });
        PlainActionFuture<NodesReloadSecureSettingsResponse> future = new PlainActionFuture<>();
        reloadSecureSettings(keyStorePassword, future);
        future.actionGet();
    }

    private void removeSecureSettingsFromKeyStoreAndReload(Set<String> settingsToRemove) {
        char[] keyStorePassword = randomAlphaOfLengthBetween(15, randomIntBetween(15, 20)).toCharArray();
        internalCluster().getInstances(Environment.class).forEach(environment -> {
            final KeyStoreWrapper keyStoreWrapper = KeyStoreWrapper.create();
            settingsToRemove.forEach(keyStoreWrapper::remove);
            try {
                keyStoreWrapper.save(environment.configDir(), keyStorePassword, false);
                logger.info(keyStoreWrapper.toString());
            } catch (Exception e) {
                fail(e.getMessage());
            }
        });
        PlainActionFuture<NodesReloadSecureSettingsResponse> future = new PlainActionFuture<>();
        reloadSecureSettings(keyStorePassword, future);
        future.actionGet();
    }

    private static void reloadSecureSettings(char[] password, ActionListener<NodesReloadSecureSettingsResponse> listener) {
        final var request = new NodesReloadSecureSettingsRequest(new String[0]);
        try {
            request.setSecureStorePassword(new SecureString(password));
            clusterAdmin().execute(TransportNodesReloadSecureSettingsAction.TYPE, request, listener);
        } finally {
            request.decRef();
        }
    }

    @Override
    public boolean transportSSLEnabled() {
        // Needs to be enabled to allow updates to secure settings
        return true;
    }
}
