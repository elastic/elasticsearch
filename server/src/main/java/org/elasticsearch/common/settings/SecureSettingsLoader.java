/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.settings;

import org.elasticsearch.cli.Terminal;

import java.nio.file.Path;

/**
 * An interface for implementing {@link SecureSettings} loaders, that is, implementations that create, initialize and load
 * secrets stores.
 */
public interface SecureSettingsLoader {
    SecureSettings load(
        Settings nodeSettings,
        Path configFile,
        Terminal terminal,
        AutoConfigureConsumer<SecureSettings, SecureString, Exception> autoConfigure
    ) throws Exception;
}
