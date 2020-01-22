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
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.KeyStoreAwareCommand;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.env.Environment;

import java.nio.file.Path;

public abstract class BaseKeyStoreCommand extends KeyStoreAwareCommand {

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
                    throw new UserException(
                        ExitCodes.DATA_ERROR,
                        "Elasticsearch keystore not found at ["
                            + KeyStoreWrapper.keystorePath(env.configFile())
                            + "]. Use 'create' command to create one."
                    );
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
     * This is called after the keystore password has been read from the stdin and the keystore is decrypted and
     * loaded. The keystore and keystore passwords are available to classes extending {@link BaseKeyStoreCommand}
     * using {@link BaseKeyStoreCommand#getKeyStore()} and {@link BaseKeyStoreCommand#getKeyStorePassword()}
     * respectively.
     */
    protected abstract void executeCommand(Terminal terminal, OptionSet options, Environment env) throws Exception;
}
