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

package org.elasticsearch.common.settings;

import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.elasticsearch.cli.EnvironmentAwareCommand;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.env.Environment;

import java.nio.file.Path;
import java.util.Arrays;

public abstract class BaseKeyStoreCommand extends EnvironmentAwareCommand {

    private KeyStoreWrapper keyStore;
    private SecureString keyStorePassword;
    private final boolean keyStoreMustExist;
    OptionSpec<Void> forceOption;

    public BaseKeyStoreCommand(String description, boolean keyStoreMustExist) {
        super(description);
        this.keyStoreMustExist = keyStoreMustExist;
    }

    @Override
    protected final void execute(Terminal terminal, OptionSet options, Environment env) throws Exception {
        try {
            final Path configFile = env.configFile();
            keyStore = KeyStoreWrapper.load(configFile);
            if (keyStore == null) {
                if (keyStoreMustExist) {
                    throw new UserException(ExitCodes.DATA_ERROR, "Elasticsearch keystore not found at [" +
                        KeyStoreWrapper.keystorePath(env.configFile()) + "]. Use 'create' command to create one.");
                } else if (options.has(forceOption) == false) {
                    if (terminal.promptYesNo("The elasticsearch keystore does not exist. Do you want to create it?", false) == false) {
                        terminal.println("Exiting without creating keystore.");
                        return;
                    }
                }
                keyStorePassword = new SecureString(new char[0]);
                keyStore = KeyStoreWrapper.create();
                keyStore.save(configFile, keyStorePassword.getChars());
            } else {
                keyStorePassword = keyStore.hasPassword() ? readPassword(terminal, false) : new SecureString(new char[0]);
                keyStore.decrypt(keyStorePassword.getChars());
            }
            executeCommand(terminal, options, env);
        } catch (SecurityException e) {
            throw new UserException(ExitCodes.DATA_ERROR, e.getMessage());
        } finally {
            if (keyStorePassword != null) {
                keyStorePassword.close();
            }
        }
    }

    protected KeyStoreWrapper getKeyStore() {
        return keyStore;
    }

    protected SecureString getKeyStorePassword() {
        return keyStorePassword;
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
    static SecureString readPassword(Terminal terminal, boolean withVerification) throws UserException {
        final char[] passwordArray;
        if (withVerification) {
            passwordArray = terminal.readSecret("Enter new password for the elasticsearch keystore (empty for no password): ");
            char[] passwordVerification = terminal.readSecret("Enter same password again: ");
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
     * This is called after the keystore password has been read from the stdin and the keystore is decrypted and
     * loaded. The keystore and keystore passwords are available to classes extending {@link BaseKeyStoreCommand}
     * using {@link BaseKeyStoreCommand#getKeyStore()} and {@link BaseKeyStoreCommand#getKeyStorePassword()}
     * respectively.
     */
    protected abstract void executeCommand(Terminal terminal, OptionSet options, Environment env) throws Exception;

    public abstract class ForceableBaseKeyStoreCommand extends BaseKeyStoreCommand {

        public ForceableBaseKeyStoreCommand(String description, boolean keyStoreMustExist) {
            super(description, keyStoreMustExist);
        }
    }
}
