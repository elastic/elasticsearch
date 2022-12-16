/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.settings;

import org.elasticsearch.cli.Terminal;

import java.nio.file.Path;

/**
 * Implementation of {@link SecureSettingsLoader} for {@link KeyStoreWrapper}
 */
public class KeyStoreLoader implements SecureSettingsLoader {
    @Override
    public SecureSettings load(
        Settings nodeSettings,
        Path configFile,
        Terminal terminal,
        AutoConfigureConsumer<SecureSettings, SecureString, Exception> autoConfigure
    ) throws Exception {
        // See if we have a keystore already present
        KeyStoreWrapper secureSettings = KeyStoreWrapper.load(configFile);
        // If there's no keystore or the keystore has no password, set an empty password
        SecureString password = (secureSettings == null || secureSettings.hasPassword() == false)
            ? new SecureString(new char[0])
            : new SecureString(terminal.readSecret(KeyStoreWrapper.PROMPT));
        // Call the init method function, e.g. allow external callers to perform additional initialization
        autoConfigure.accept(secureSettings, password);
        // Bootstrap a keystore, at this point if no keystore existed it will be created.
        // Bootstrap will decrypt the keystore if it was pre-existing.
        secureSettings = KeyStoreWrapper.bootstrap(configFile, () -> password);
        // We don't need the password anymore, close it
        password.close();

        return secureSettings;
    }
}
