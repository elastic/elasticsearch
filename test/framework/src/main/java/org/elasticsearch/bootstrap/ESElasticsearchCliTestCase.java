/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.bootstrap;

import org.elasticsearch.cli.MockTerminal;
import org.elasticsearch.test.ESTestCase;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static org.hamcrest.CoreMatchers.equalTo;

abstract class ESElasticsearchCliTestCase extends ESTestCase {

    interface InitConsumer {
        void accept(final boolean foreground, final String pidFile, final Map<String, String> esSettings);
    }

    void runTest(
            final int expectedStatus,
            final boolean expectedInit,
            final Consumer<String> outputConsumer,
            final InitConsumer initConsumer,
            String... args) throws Exception {
        final MockTerminal terminal = new MockTerminal();
        try {
            final AtomicBoolean init = new AtomicBoolean();
            final int status = Elasticsearch.main(args, new Elasticsearch() {
                @Override
                void init(final boolean daemonize, final String pidFile, final Map<String, String> esSettings) {
                    init.set(true);
                    initConsumer.accept(!daemonize, pidFile, esSettings);
                }
            }, terminal);
            assertThat(status, equalTo(expectedStatus));
            assertThat(init.get(), equalTo(expectedInit));
            outputConsumer.accept(terminal.getOutput());
        } catch (Throwable t) {
            // if an unexpected exception is thrown, we log
            // terminal output to aid debugging
            logger.info(terminal.getOutput());
            // rethrow so the test fails
            throw t;
        }
    }

}
