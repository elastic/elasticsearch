/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.settings;

import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import org.elasticsearch.cli.Command;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.ProcessInfo;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.common.cli.EnvironmentAwareCommand;
import org.elasticsearch.env.Environment;

import java.io.Closeable;
import java.io.OutputStream;
import java.util.Optional;

/**
 * Implementation of {@link SecureSettingsLoader} for {@link KeyStoreWrapper}
 */
public class KeyStoreLoader implements SecureSettingsLoader {
    @Override
    public LoadedSecrets load(
        Environment environment,
        Terminal terminal,
        ProcessInfo processInfo,
        OptionSet options,
        Command autoConfigureCommand,
        OptionSpec<String> enrollmentTokenOption
    ) throws Exception {
        // See if we have a keystore already present
        KeyStoreWrapper secureSettings = KeyStoreWrapper.load(environment.configFile());
        // If there's no keystore or the keystore has no password, set an empty password
        SecureString password = (secureSettings == null || secureSettings.hasPassword() == false)
            ? new SecureString(new char[0])
            : new SecureString(terminal.readSecret(KeyStoreWrapper.PROMPT));
        // Call the init method function, e.g. allow external callers to perform additional initialization
        Optional<OptionSet> modifiedOptions = autoConfigureSecurity(
            terminal,
            options,
            processInfo,
            environment,
            password,
            autoConfigureCommand,
            enrollmentTokenOption
        );
        // Bootstrap a keystore, at this point if no keystore existed it will be created.
        // Bootstrap will decrypt the keystore if it was pre-existing.
        secureSettings = KeyStoreWrapper.bootstrap(environment.configFile(), () -> password);
        // We don't need the password anymore, close it
        password.close();

        return new LoadedSecrets(secureSettings, modifiedOptions);
    }

    private Optional<OptionSet> autoConfigureSecurity(
        Terminal terminal,
        OptionSet options,
        ProcessInfo processInfo,
        Environment env,
        SecureString keystorePassword,
        Command cmd,
        OptionSpec<String> enrollmentTokenOption
    ) throws Exception {
        if (cmd == null) {
            return Optional.empty();
        }

        @SuppressWarnings("raw")
        var autoConfigNode = (EnvironmentAwareCommand) cmd;
        final String[] autoConfigArgs;
        if (options.has(enrollmentTokenOption)) {
            autoConfigArgs = new String[] { "--enrollment-token", options.valueOf(enrollmentTokenOption) };
        } else {
            autoConfigArgs = new String[0];
        }
        OptionSet autoConfigOptions = autoConfigNode.parseOptions(autoConfigArgs);

        boolean changed = true;
        try (var autoConfigTerminal = new KeystorePasswordTerminal(terminal, keystorePassword.clone())) {
            autoConfigNode.execute(autoConfigTerminal, autoConfigOptions, env, processInfo);
        } catch (UserException e) {
            boolean okCode = switch (e.exitCode) {
                // these exit codes cover the cases where auto-conf cannot run but the node should NOT be prevented from starting as usual
                // e.g. the node is restarted, is already configured in an incompatible way, or the file system permissions do not allow it
                case ExitCodes.CANT_CREATE, ExitCodes.CONFIG, ExitCodes.NOOP -> true;
                default -> false;
            };
            if (options.has(enrollmentTokenOption) == false && okCode) {
                // we still want to print the error, just don't fail startup
                if (e.getMessage() != null) {
                    terminal.errorPrintln(e.getMessage());
                }
                changed = false;
            } else {
                throw e;
            }
        }
        if (changed) {
            // reload settings since auto security changed them
            return Optional.of(options);
        }
        return Optional.empty();
    }

    /**
     * A terminal that wraps an existing terminal and provides a single secret input, the keystore password.
     */
    class KeystorePasswordTerminal extends Terminal implements Closeable {

        private final Terminal delegate;
        private final SecureString password;

        KeystorePasswordTerminal(Terminal delegate, SecureString password) {
            super(delegate.getReader(), delegate.getWriter(), delegate.getErrorWriter());
            this.delegate = delegate;
            this.password = password;
            setVerbosity(delegate.getVerbosity());
        }

        @Override
        public char[] readSecret(String prompt) {
            return password.getChars();
        }

        @Override
        public OutputStream getOutputStream() {
            return delegate.getOutputStream();
        }

        @Override
        public void close() {
            password.close();
        }
    }
}
