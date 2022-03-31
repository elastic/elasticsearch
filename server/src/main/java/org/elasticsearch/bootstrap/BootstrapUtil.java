/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.bootstrap;

import org.elasticsearch.cli.Terminal;
import org.elasticsearch.common.settings.KeyStoreWrapper;
import org.elasticsearch.common.settings.SecureSettings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.env.Environment;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

/**
 * Utilities for use during bootstrap. This is public so that tests may use these methods.
 */
public class BootstrapUtil {

    // no construction
    private BootstrapUtil() {}

    /**
     * Read from an InputStream up to the first carriage return or newline,
     * returning no more than maxLength characters.
     */
    public static SecureString readPassphrase(InputStream stream, int maxLength) throws IOException {
        SecureString passphrase;

        try (InputStreamReader reader = new InputStreamReader(stream, StandardCharsets.UTF_8)) {
            passphrase = new SecureString(Terminal.readLineToCharArray(reader, maxLength));
        } catch (RuntimeException e) {
            if (e.getMessage().startsWith("Input exceeded maximum length")) {
                throw new IllegalStateException("Password exceeded maximum length of " + maxLength, e);
            }
            throw e;
        }

        if (passphrase.length() == 0) {
            passphrase.close();
            throw new IllegalStateException("Keystore passphrase required but none provided.");
        }

        return passphrase;
    }

    public static SecureSettings loadSecureSettings(Environment initialEnv) throws BootstrapException {
        return loadSecureSettings(initialEnv, System.in);
    }

    public static SecureSettings loadSecureSettings(Environment initialEnv, InputStream stdin) throws BootstrapException {
        try {
            return KeyStoreWrapper.bootstrap(initialEnv.configFile(), () -> readPassphrase(stdin, KeyStoreWrapper.MAX_PASSPHRASE_LENGTH));
        } catch (Exception e) {
            throw new BootstrapException(e);
        }
    }
}
