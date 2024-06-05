/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.cli;

import joptsimple.OptionSet;

import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.ProcessInfo;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.common.settings.KeyStoreWrapper;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.env.Environment;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Arrays;

/**
 * An {@link EnvironmentAwareCommand} that needs to access the elasticsearch keystore, possibly
 * decrypting it if it is password protected.
 */
public abstract class KeyStoreAwareCommand extends EnvironmentAwareCommand {
    public KeyStoreAwareCommand(String description) {
        super(description);
    }

    /**
     * Reads the keystore password from the {@link Terminal}, prompting for verification where applicable and returns it as a
     * {@link SecureString}.
     *
     * @param terminal         the terminal to use for user inputs
     * @param withVerification whether the user should be prompted for password verification
     * @return a SecureString with the password the user entered
     * @throws UserException If the user is prompted for verification and enters a different password
     */
    protected static SecureString readPassword(Terminal terminal, boolean withVerification) throws UserException {
        final char[] passwordArray;
        if (withVerification) {
            passwordArray = terminal.readSecret("Enter new password for the elasticsearch keystore (empty for no password): ");
            char[] passwordVerification = terminal.readSecret("Enter same password again: ");
            if (Arrays.equals(passwordArray, passwordVerification) == false) {
                throw new UserException(
                    ExitCodes.DATA_ERROR,
                    "Passwords are not equal, exiting.: " + new String(passwordArray) + ", " + new String(passwordVerification)
                );
            }
            Arrays.fill(passwordVerification, '\u0000');
        } else {
            passwordArray = terminal.readSecret(KeyStoreWrapper.PROMPT);
        }
        return new SecureString(passwordArray);
    }

    /**
     * Decrypt the {@code keyStore}, prompting the user to enter the password in the {@link Terminal} if it is password protected
     */
    protected static void decryptKeyStore(KeyStoreWrapper keyStore, Terminal terminal) throws UserException, IOException {
        try (SecureString keystorePassword = keyStore.hasPassword() ? readPassword(terminal, false) : new SecureString(new char[0])) {
            keyStore.decrypt(keystorePassword.getChars());
        } catch (SecurityException | GeneralSecurityException e) {
            throw new UserException(ExitCodes.DATA_ERROR, e.getMessage());
        }
    }

    public abstract void execute(Terminal terminal, OptionSet options, Environment env, ProcessInfo processInfo) throws Exception;
}
