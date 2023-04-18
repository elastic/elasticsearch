/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cli.keystore;

import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.ProcessInfo;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.common.cli.KeyStoreAwareCommand;
import org.elasticsearch.common.settings.KeyStoreWrapper;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.env.Environment;

import java.nio.file.Path;
import java.security.GeneralSecurityException;

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
    public final void execute(Terminal terminal, OptionSet options, Environment env, ProcessInfo processInfo) throws Exception {
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
        } catch (SecurityException | GeneralSecurityException e) {
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
