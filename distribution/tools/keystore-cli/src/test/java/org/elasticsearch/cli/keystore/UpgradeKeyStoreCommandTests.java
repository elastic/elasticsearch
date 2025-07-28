/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cli.keystore;

import joptsimple.OptionSet;

import org.elasticsearch.cli.Command;
import org.elasticsearch.cli.ProcessInfo;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.common.settings.KeyStoreWrapper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.env.Environment;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasToString;

public class UpgradeKeyStoreCommandTests extends KeyStoreCommandTestCase {

    @Override
    protected Command newCommand() {
        return new UpgradeKeyStoreCommand() {
            @Override
            protected Environment createEnv(OptionSet options, ProcessInfo processInfo) {
                return env;
            }
        };
    }

    public void testKeystoreUpgradeV3() throws Exception {
        assertKeystoreUpgrade("/format-v3-elasticsearch.keystore", KeyStoreWrapper.V3_VERSION);
    }

    public void testKeystoreUpgradeV4() throws Exception {
        assertKeystoreUpgrade("/format-v4-elasticsearch.keystore", KeyStoreWrapper.V4_VERSION);
    }

    public void testKeystoreUpgradeV5() throws Exception {
        assertKeystoreUpgradeWithPassword("/format-v5-with-password-elasticsearch.keystore", KeyStoreWrapper.LE_VERSION);
    }

    private void assertKeystoreUpgrade(String file, int version) throws Exception {
        assumeFalse("Cannot open unprotected keystore on FIPS JVM", inFipsJvm());
        assertKeystoreUpgrade(file, version, null);
    }

    private void assertKeystoreUpgradeWithPassword(String file, int version) throws Exception {
        assertKeystoreUpgrade(file, version, "keystorepassword");
    }

    private void assertKeystoreUpgrade(String file, int version, @Nullable String password) throws Exception {
        final Path keystore = KeyStoreWrapper.keystorePath(env.configDir());
        try (InputStream is = KeyStoreWrapperTests.class.getResourceAsStream(file); OutputStream os = Files.newOutputStream(keystore)) {
            is.transferTo(os);
        }
        try (KeyStoreWrapper beforeUpgrade = KeyStoreWrapper.load(env.configDir())) {
            assertNotNull(beforeUpgrade);
            assertThat(beforeUpgrade.getFormatVersion(), equalTo(version));
        }
        if (password != null) {
            terminal.addSecretInput(password);
            terminal.addSecretInput(password);
        }
        execute();
        terminal.reset();

        try (KeyStoreWrapper afterUpgrade = KeyStoreWrapper.load(env.configDir())) {
            assertNotNull(afterUpgrade);
            assertThat(afterUpgrade.getFormatVersion(), equalTo(KeyStoreWrapper.CURRENT_VERSION));
            afterUpgrade.decrypt(password != null ? password.toCharArray() : new char[0]);
            assertThat(afterUpgrade.getSettingNames(), hasItem(KeyStoreWrapper.SEED_SETTING.getKey()));
        }
    }

    public void testKeystoreDoesNotExist() {
        final UserException e = expectThrows(UserException.class, this::execute);
        assertThat(e, hasToString(containsString("keystore not found at [" + KeyStoreWrapper.keystorePath(env.configDir()) + "]")));
    }
}
