/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.settings.secure;

import org.elasticsearch.common.settings.ClusterSecrets;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.SecureClusterStateSettings;
import org.elasticsearch.common.settings.SecureSetting;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.core.Strings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.encryption.spi.EncryptedData;
import org.elasticsearch.xpack.encryption.spi.EncryptionService;
import org.elasticsearch.xpack.encryption.spi.EncryptionServiceState;
import org.elasticsearch.xpack.encryption.spi.EncryptionServiceUnavailableException;
import org.junit.Before;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class SecureSettingsReloaderTests extends ESTestCase {

    private static final Setting<SecureString> SETTING = SecureSetting.secureString("test.secret", null);

    private DeterministicTaskQueue taskQueue;
    private FakeEncryptionService encryptionService;
    private List<String> reloadedValues;

    @Before
    public void setUpReloader() {
        taskQueue = new DeterministicTaskQueue();
        encryptionService = new FakeEncryptionService();
        reloadedValues = new ArrayList<>();
    }

    public void testReloadsOnlyLatestPendingSourcesOffTheCallingThread() {
        SecureSettingsReloader reloader = reloader(null);

        reloader.reloadAsync(new SecureSettingsReloader.Sources(null, apiSettings("first")));
        reloader.reloadAsync(new SecureSettingsReloader.Sources(null, apiSettings("second")));
        assertThat(reloadedValues, empty());
        taskQueue.runAllTasks();

        assertThat(reloadedValues, contains("second"));
    }

    public void testPrecedence() throws IOException {
        MockSecureSettings startupKeystore = keystore("keystore");
        SecureSettingsReloader reloader = reloader(startupKeystore);
        startupKeystore.close();

        reloader.reloadAsync(new SecureSettingsReloader.Sources(null, null));
        taskQueue.runAllTasks();
        reloader.reloadAsync(new SecureSettingsReloader.Sources(null, apiSettings("api")));
        taskQueue.runAllTasks();
        reloader.reloadAsync(new SecureSettingsReloader.Sources(clusterSecrets("reserved"), apiSettings("api")));
        taskQueue.runAllTasks();

        assertThat(reloadedValues, contains("keystore", "api", "reserved"));
    }

    public void testWipedValueDoesNotFallBackToKeystore() {
        SecureSettingsReloader reloader = reloader(keystore("keystore"));
        Map<String, EncryptedData> wiped = new HashMap<>();
        wiped.put(SETTING.getKey(), null);

        reloader.reloadAsync(new SecureSettingsReloader.Sources(null, new SecureSettingsMetadata(wiped)));
        taskQueue.runAllTasks();

        assertThat(reloadedValues.size(), equalTo(1));
        assertThat(reloadedValues.get(0), nullValue());
    }

    public void testReloadWithKeystoreReplacesKeystoreForLaterReloads() throws Exception {
        SecureSettingsReloader reloader = reloader(keystore("old"));
        List<String> reloadSecureSettingsValues = new ArrayList<>();

        MockSecureSettings newKeystore = keystore("new");
        reloader.reloadWithKeystore(
            newKeystore,
            new SecureSettingsReloader.Sources(null, apiSettings("api")),
            settings -> reloadSecureSettingsValues.add(value(settings))
        );
        newKeystore.close();
        assertThat(reloadSecureSettingsValues, contains("api"));

        reloader.reloadAsync(new SecureSettingsReloader.Sources(null, null));
        taskQueue.runAllTasks();
        assertThat(reloadedValues, contains("new"));
    }

    public void testReloadWithKeystoreKeepsPreviousKeystoreWhenApiValuesCannotBeDecrypted() throws Exception {
        SecureSettingsReloader reloader = reloader(keystore("old"));
        SecureSettingsMetadata apiSettings = apiSettings("api");
        encryptionService.available = false;

        expectThrows(
            EncryptionServiceUnavailableException.class,
            () -> reloader.reloadWithKeystore(
                keystore("new"),
                new SecureSettingsReloader.Sources(null, apiSettings),
                settings -> fail("must not reload")
            )
        );

        reloader.reloadAsync(new SecureSettingsReloader.Sources(null, null));
        taskQueue.runAllTasks();
        assertThat(reloadedValues, contains("old"));
    }

    public void testRetryNeededWhileApiValuesCannotBeDecrypted() {
        SecureSettingsReloader reloader = reloader(keystore("keystore"));
        SecureSettingsMetadata apiSettings = apiSettings("api");
        encryptionService.available = false;

        reloader.reloadAsync(new SecureSettingsReloader.Sources(null, apiSettings));
        taskQueue.runAllTasks();
        assertThat(reloadedValues, empty());
        assertTrue(reloader.retryNeeded());

        encryptionService.available = true;
        reloader.reloadAsync(new SecureSettingsReloader.Sources(null, apiSettings));
        taskQueue.runAllTasks();
        assertThat(reloadedValues, contains("api"));
        assertFalse(reloader.retryNeeded());
    }

    private SecureSettingsReloader reloader(MockSecureSettings keystore) {
        SecureSettingsReloader reloader = new SecureSettingsReloader(
            Settings.EMPTY,
            keystore,
            encryptionService,
            taskQueue.getThreadPool().generic()
        );
        reloader.setReloadCallback(settings -> reloadedValues.add(value(settings)));
        return reloader;
    }

    private static String value(Settings settings) {
        try (SecureString value = SETTING.get(settings)) {
            return value.length() == 0 ? null : value.toString();
        }
    }

    private static MockSecureSettings keystore(String value) {
        MockSecureSettings keystore = new MockSecureSettings();
        keystore.setString(SETTING.getKey(), value);
        return keystore;
    }

    private SecureSettingsMetadata apiSettings(String value) {
        return new SecureSettingsMetadata(Map.of(SETTING.getKey(), encryptionService.encrypt(value.getBytes(StandardCharsets.UTF_8))));
    }

    private ClusterSecrets clusterSecrets(String value) throws IOException {
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, Strings.format("""
            {"string_secrets": {"%s": "%s"}}""", SETTING.getKey(), value))) {
            return new ClusterSecrets(1L, SecureClusterStateSettings.fromXContent(parser));
        }
    }

    /**
     * Stands in for the encryption plugin's AES-GCM service, which needs a project encryption key; the reloader only depends on
     * decryption returning the plaintext or throwing while the service is unavailable.
     */
    private static class FakeEncryptionService implements EncryptionService {

        volatile boolean available = true;

        @Override
        public EncryptedData encrypt(byte[] bytes) {
            return new EncryptedData("test-key", bytes.clone());
        }

        @Override
        public byte[] decrypt(EncryptedData encryptedData) {
            if (available == false) {
                throw new EncryptionServiceUnavailableException(EncryptionServiceState.DISABLED);
            }
            return encryptedData.payload().clone();
        }
    }
}
