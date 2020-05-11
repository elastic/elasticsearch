/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.cli;

import joptsimple.OptionSet;
import org.elasticsearch.common.settings.KeyStoreWrapper;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.env.Environment;

import javax.crypto.AEADBadTagException;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Arrays;

/**
 * An {@link org.elasticsearch.cli.EnvironmentAwareCommand} that needs to access the elasticsearch keystore, possibly
 * decrypting it if it is password protected.
 */
public abstract class KeyStoreAwareCommand extends EnvironmentAwareCommand {
    public KeyStoreAwareCommand(String description) {
        super(description);
    }

    /** Arbitrarily chosen maximum passphrase length */
    public static final int MAX_PASSPHRASE_LENGTH = 128;

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
            passwordArray = terminal.readSecret("Enter new password for the elasticsearch keystore (empty for no password): ",
                MAX_PASSPHRASE_LENGTH);
            char[] passwordVerification = terminal.readSecret("Enter same password again: ",
                MAX_PASSPHRASE_LENGTH);
            if (Arrays.equals(passwordArray, passwordVerification) == false) {
                throw new UserException(ExitCodes.DATA_ERROR, "Passwords are not equal, exiting.");
            }
            Arrays.fill(passwordVerification, '\u0000');
        } else {
            passwordArray = terminal.readSecret("Enter password for the elasticsearch keystore : ");
        }
        return new SecureString(passwordArray);
    }

    /**
     * Decrypt the {@code keyStore}, prompting the user to enter the password in the {@link Terminal} if it is password protected
     */
    protected static void decryptKeyStore(KeyStoreWrapper keyStore, Terminal terminal)
        throws UserException, GeneralSecurityException, IOException {
        try (SecureString keystorePassword = keyStore.hasPassword() ?
            readPassword(terminal, false) : new SecureString(new char[0])) {
            keyStore.decrypt(keystorePassword.getChars());
        } catch (SecurityException e) {
            if (e.getCause() instanceof AEADBadTagException) {
                throw new UserException(ExitCodes.DATA_ERROR, "Wrong password for elasticsearch.keystore");
            }
        }
    }

    protected abstract void execute(Terminal terminal, OptionSet options, Environment env) throws Exception;
}
