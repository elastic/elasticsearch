/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.server.cli;

import org.elasticsearch.cli.Terminal;
import org.elasticsearch.common.settings.SecureSettings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.env.Environment;

import java.util.Optional;

/**
 * An interface for implementing {@link SecureSettings} loaders, that is, implementations that create, initialize and load
 * secrets stores.
 */
public interface SecureSettingsLoader {
    /**
     * Loads an existing SecureSettings implementation
     */
    LoadedSecrets load(Environment environment, Terminal terminal) throws Exception;

    /**
     * Loads an existing SecureSettings implementation, creates one if it doesn't exist
     */
    SecureSettings bootstrap(Environment environment, SecureString password) throws Exception;

    /**
     * A load result for loading a SecureSettings implementation from a SecureSettingsLoader
     * @param secrets the loaded secure settings
     * @param password an optional password if the implementation required one
     */
    record LoadedSecrets(SecureSettings secrets, Optional<SecureString> password) implements AutoCloseable {
        @Override
        public void close() throws Exception {
            if (password.isPresent()) {
                password.get().close();
            }
        }
    }

    boolean supportsSecurityAutoConfiguration();
}
