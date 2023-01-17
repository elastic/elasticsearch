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
public interface SecureSettingsLoader {
    SecureSettings load(Environment environment, Terminal terminal, AutoConfigureFunction<SecureString, Environment> autoConfigure)
        throws Exception;

    default AutoConfigureResult autoConfigure(
        Environment env,
        Terminal terminal,
        ZonedDateTime autoConfigDate,
        Consumer<SecureString> configureTransportSecrets,
        Consumer<SecureString> configureHttpSecrets
    ) {
        return null;
    }

    default Exception removeAutoConfiguration(Environment environment, Terminal terminal) {
        return null;
    }

    String validate(Environment environment);

    /**
     * Functional interface for implementing an autoconfigure call-back from secrets loader implementations
     * @param <T> argument
     * @param <R> return type
     */
    @FunctionalInterface
    interface AutoConfigureFunction<T, R> {
        R apply(T t) throws Exception;
    }

    /**
     * Functional interface for implementing autoconfigure call-backs during autoconfigure, e.g. onSuccess and onFailure.
     * @param <T> argument
     */
    @FunctionalInterface
    interface AutoConfigureReponse<T> {
        void apply(T t) throws Exception;
    }

    /**
     * Holder record for results of an autoconfigure call on a secrets loader. The callbacks are there to allow
     * for custom clean-up to run on success or failure, if the secrets loader autoconfigure code has any intermediate
     * state.
     *
     * @param autoConfigureError any encountered errors during secrets loader custom autoconfigure logic
     * @param onSuccess a callback for the main autoconfigure code to notify the loader it succeeded
     * @param onFailure a callback for the main autoconfigure code to notify the loader it failed
     */
    record AutoConfigureResult(
        Exception autoConfigureError,
        AutoConfigureReponse<Environment> onSuccess,
        AutoConfigureReponse<Environment> onFailure
    ) {}
}
