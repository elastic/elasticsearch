/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.settings;

import org.elasticsearch.cli.Command;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.env.Environment;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasToString;

public class UpgradeKeyStoreCommandTests extends KeyStoreCommandTestCase {

    @Override
    protected Command newCommand() {
        return new UpgradeKeyStoreCommand() {

            @Override
            protected Environment createEnv(final Map<String, String> settings) {
                return env;
            }

        };
    }

    public void testKeystoreUpgrade() throws Exception {
        assumeFalse("Cannot open unprotected keystore on FIPS JVM", inFipsJvm());
        final Path keystore = KeyStoreWrapper.keystorePath(env.configFile());
        try (
            InputStream is = KeyStoreWrapperTests.class.getResourceAsStream("/format-v3-elasticsearch.keystore");
            OutputStream os = Files.newOutputStream(keystore)
        ) {
            is.transferTo(os);
        }
        try (KeyStoreWrapper beforeUpgrade = KeyStoreWrapper.load(env.configFile())) {
            assertNotNull(beforeUpgrade);
            assertThat(beforeUpgrade.getFormatVersion(), equalTo(3));
        }
        execute();
        try (KeyStoreWrapper afterUpgrade = KeyStoreWrapper.load(env.configFile())) {
            assertNotNull(afterUpgrade);
            assertThat(afterUpgrade.getFormatVersion(), equalTo(KeyStoreWrapper.FORMAT_VERSION));
            afterUpgrade.decrypt(new char[0]);
            assertThat(afterUpgrade.getSettingNames(), hasItem(KeyStoreWrapper.SEED_SETTING.getKey()));
        }
    }

    public void testKeystoreDoesNotExist() {
        final UserException e = expectThrows(UserException.class, this::execute);
        assertThat(e, hasToString(containsString("keystore not found at [" + KeyStoreWrapper.keystorePath(env.configFile()) + "]")));
    }

}
