package org.elasticsearch.common.settings;

import joptsimple.OptionSet;
import org.elasticsearch.cli.KeyStoreAwareCommand;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.env.Environment;

import java.nio.file.Path;

public class HasPasswordKeyStoreCommand extends KeyStoreAwareCommand {

    static final int NO_PASSWORD_EXIT_CODE = 1;

    HasPasswordKeyStoreCommand() {
        super("Succeeds if the keystore exists and is password-protected, fails with exit code 1 otherwise.");
    }

    @Override
    protected void execute(Terminal terminal, OptionSet options, Environment env) throws Exception {
        final Path configFile = env.configFile();
        final KeyStoreWrapper keyStore = KeyStoreWrapper.load(configFile);
        if (keyStore == null) {
            throw new UserException(NO_PASSWORD_EXIT_CODE, "Elasticsearch keystore not found");
        }
        if (keyStore.hasPassword()) {
            terminal.println(Terminal.Verbosity.NORMAL, "Keystore is password-protected");
            return;
        }
        throw new UserException(NO_PASSWORD_EXIT_CODE, "Keystore is password protected");
    }
}
