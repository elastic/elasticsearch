/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.bootstrap;

import org.elasticsearch.cli.MockTerminal;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ESTestCase;

import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

import static org.hamcrest.CoreMatchers.equalTo;

abstract class ESElasticsearchCliTestCase extends ESTestCase {

    interface InitConsumer {
        void accept(boolean foreground, Path pidFile, boolean quiet, Environment initialEnv);
    }

    void runTest(
            final int expectedStatus,
            final boolean expectedInit,
            final BiConsumer<String, String> outputConsumer,
            final InitConsumer initConsumer,
            final String... args) throws Exception {
        final MockTerminal terminal = new MockTerminal();
        final Path home = createTempDir();
        try {
            final AtomicBoolean init = new AtomicBoolean();
            final int status = Elasticsearch.main(args, new Elasticsearch() {
                @Override
                protected Environment createEnv(final Map<String, String> settings) throws UserException {
                    Settings.Builder builder = Settings.builder().put("path.home", home);
                    settings.forEach((k,v) -> builder.put(k, v));
                    final Settings realSettings = builder.build();
                    return new Environment(realSettings, home.resolve("config"));
                }
                @Override
                void init(final boolean daemonize, final Path pidFile, final boolean quiet, Environment initialEnv) {
                    init.set(true);
                    initConsumer.accept(daemonize == false, pidFile, quiet, initialEnv);
                }

                @Override
                protected boolean addShutdownHook() {
                    return false;
                }
            }, terminal);
            assertThat(status, equalTo(expectedStatus));
            assertThat(init.get(), equalTo(expectedInit));
            outputConsumer.accept(terminal.getOutput(), terminal.getErrorOutput());
        } catch (Exception e) {
            // if an unexpected exception is thrown, we log
            // terminal output to aid debugging
            logger.info("Stdout:\n" + terminal.getOutput());
            logger.info("Stderr:\n" + terminal.getErrorOutput());
            // rethrow so the test fails
            throw e;
        }
    }

}
