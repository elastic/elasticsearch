/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.settings;

import org.elasticsearch.cli.LoggingAwareMultiCommand;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.common.settings.cli.ShowKeyStoreCommand;

/**
 * A cli tool for managing secrets in the elasticsearch keystore.
 */
public class KeyStoreCli extends LoggingAwareMultiCommand {

    private KeyStoreCli() {
        super("A tool for managing settings stored in the elasticsearch keystore");
        subcommands.put("create", new CreateKeyStoreCommand());
        subcommands.put("list", new ListKeyStoreCommand());
        subcommands.put("show", new ShowKeyStoreCommand());
        subcommands.put("add", new AddStringKeyStoreCommand());
        subcommands.put("add-file", new AddFileKeyStoreCommand());
        subcommands.put("remove", new RemoveSettingKeyStoreCommand());
        subcommands.put("upgrade", new UpgradeKeyStoreCommand());
        subcommands.put("passwd", new ChangeKeyStorePasswordCommand());
        subcommands.put("has-passwd", new HasPasswordKeyStoreCommand());
    }

    public static void main(String[] args) throws Exception {
        exit(new KeyStoreCli().main(args, Terminal.DEFAULT));
    }

}
