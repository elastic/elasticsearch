/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.server.cli;

import org.elasticsearch.cli.Terminal;
import org.elasticsearch.common.settings.KeyStoreWrapper;
import org.elasticsearch.common.settings.SecureSettings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.env.Environment;

import java.util.Optional;

/**
 * Implementation of {@link SecureSettingsLoader} for {@link KeyStoreWrapper}
 */
public class KeyStoreLoader implements SecureSettingsLoader {
    @Override
    public LoadedSecrets load(Environment environment, Terminal terminal) throws Exception {
        // See if we have a keystore already present
        KeyStoreWrapper secureSettings = KeyStoreWrapper.load(environment.configFile());
        // If there's no keystore or the keystore has no password, set an empty password
        var password = (secureSettings == null || secureSettings.hasPassword() == false)
            ? new SecureString(new char[0])
            : new SecureString(terminal.readSecret(KeyStoreWrapper.PROMPT));

        return new LoadedSecrets(secureSettings, Optional.of(password));
    }

    @Override
    public SecureSettings bootstrap(Environment environment, SecureString password) throws Exception {
        return KeyStoreWrapper.bootstrap(environment.configFile(), () -> password);
    }

    @Override
    public boolean supportsSecurityAutoConfiguration() {
        return true;
    }
}
