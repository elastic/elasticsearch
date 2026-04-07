/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cli.keystore;

import joptsimple.OptionSet;

import org.elasticsearch.cli.Terminal;
import org.elasticsearch.common.settings.KeyStoreWrapper;
import org.elasticsearch.env.Environment;

/**
 * A sub-command for the keystore CLI that enables upgrading the keystore format.
 */
public class UpgradeKeyStoreCommand extends BaseKeyStoreCommand {

    UpgradeKeyStoreCommand() {
        super("Upgrade the keystore format", true);
    }

    @Override
    protected void executeCommand(final Terminal terminal, final OptionSet options, final Environment env) throws Exception {
        KeyStoreWrapper.upgrade(getKeyStore(), env.configDir(), getKeyStorePassword().getChars());
    }

}
