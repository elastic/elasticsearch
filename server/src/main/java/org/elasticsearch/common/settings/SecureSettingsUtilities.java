/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.settings;

import java.nio.file.Path;

/**
 * Utility class that creates {@link SecureSettings} based on the kind of
 * secure settings implementation is configured for Elasticsearch.
 */
public class SecureSettingsUtilities {
    /**
     * Loads a SecureSettings implementation based on the node configuration.
     * If no SecureSettings implementation is found, null is returned.

     * @param nodeSettings node settings
     * @param configFile path to the configuration directory
     * @return a SecureSettings instance or null if none is found
     */
    public static SecureSettings load(Settings nodeSettings, Path configFile) throws Exception {
        // TODO: Use the settings to decide what kind of SecureSettings we'll provide
        return KeyStoreWrapper.load(configFile);
    }

    /**
     * Loads and opens a SecureSettings implementation based on the node configuration.
     * If no SecureSettings are found, this method will create a default one.
     *
     * @param nodeSettings node settings
     * @param configFile path to the configuration directory
     * @param credentials credentials to be used to open the SecureSettings store if any are
     *                    required.
     * @return a SecureSettings instance which might be a default one if none was found
     */
    public static SecureSettings bootstrap(Settings nodeSettings, Path configFile, SecureString credentials) throws Exception {
        // TODO: Use the settings to decide what kind of SecureSettings we'll provide
        return KeyStoreWrapper.bootstrap(configFile, () -> credentials);
    }
}
