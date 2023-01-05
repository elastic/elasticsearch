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
import org.elasticsearch.cli.ProcessInfo;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.env.Environment;

import java.util.Optional;

/**
 * An interface for implementing {@link SecureSettings} loaders, that is, implementations that create, initialize and load
 * secrets stores.
 */
public interface SecureSettingsLoader {
    LoadedSecrets load(
        Environment environment,
        Terminal terminal,
        ProcessInfo processInfo,
        OptionSet options,
        Command autoConfigureCommand,
        OptionSpec<String> enrollmentTokenOption
    ) throws Exception;

    /**
     * The secure settings loader will materialize the secrets in memory, however secure settings
     * loader are allowed to modify the supplied environment options and return an updated version. This happens
     * when we auto-configure security on Elasticsearch start.
     * @param secrets the in-memory loaded secrets
     * @param options optionally, modified options by auto-configuration of the node
     */
    record LoadedSecrets(SecureSettings secrets, Optional<OptionSet> options) implements AutoCloseable {
        @Override
        public void close() throws Exception {
            secrets.close();
        }
    }
}
