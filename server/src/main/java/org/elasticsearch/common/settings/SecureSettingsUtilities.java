/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.settings;

import java.nio.file.Path;

public class SecureSettingsUtilities {
    public static SecureSettings load(Settings nodeSettings, Path configFile) throws Exception {
        // TODO: Use the settings to decide what kind of SecureSettings we'll provide
        return KeyStoreWrapper.load(configFile);
    }

    public static SecureSettings bootstrap(Settings nodeSettings, Path configFile, SecureString credentials) throws Exception {
        // TODO: Use the settings to decide what kind of SecureSettings we'll provide
        return KeyStoreWrapper.bootstrap(configFile, () -> credentials);
    }

    public static void openWithCredentials(SecureSettings secrets, SecureString credentials) throws Exception {
        if (secrets == null) {
            return;
        }

        if (secrets instanceof KeyStoreWrapper keyStoreWrapper) {
            keyStoreWrapper.decrypt(credentials.getChars());
        }
    }
}
