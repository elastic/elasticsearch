/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.settings;

import org.elasticsearch.cli.Terminal;
import org.elasticsearch.env.Environment;

import java.time.ZonedDateTime;
import java.util.function.Consumer;

/**
 * An interface for implementing {@link SecureSettings} loaders, that is, implementations that create, initialize and load
 * secrets stores.
 */
public interface SecureSettingsLoader extends AutoCloseable {
    SecureSettings load(Environment environment, Terminal terminal, AutoConfigureFunction<SecureString, Environment> autoConfigure)
        throws Exception;

    default Exception autoConfigure(
        Environment env,
        Terminal terminal,
        ZonedDateTime autoConfigDate,
        Consumer<SecureString> configureTransportSecrets,
        Consumer<SecureString> configureHttpSecrets
    ) {
        return null;
    }

    default void onAutoConfigureFailure(Environment environment) throws Exception {}

    default void onAutoConfigureSuccess(Environment environment) throws Exception {}

    default Exception removeAutoConfiguration(Environment env) {
        return null;
    }

    String valid(Environment environment);

    @FunctionalInterface
    interface AutoConfigureFunction<T, R> {
        R apply(T t) throws Exception;
    }
}
