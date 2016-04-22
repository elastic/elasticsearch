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

package org.elasticsearch.plugins;

import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.MockTerminal;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ESTestCase;

import java.util.function.Consumer;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;

public class PluginCliTests extends ESTestCase {

    public void testNullArgs() throws Exception {
        runTest(
            ExitCodes.OK,
            output -> assertThat(output, containsString("A tool for managing installed elasticsearch plugins"))
        );
    }

    public void testNoArgs() throws Exception {
        runTest(
            ExitCodes.OK,
            output -> assertThat(output, containsString("A tool for managing installed elasticsearch plugins")),
            new String[0]
        );
    }

    private void runTest(
        final int expectedStatus,
        final Consumer<String> outputConsumer,
        String... args) throws Exception {
        final MockTerminal terminal = new MockTerminal();
        Settings build = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toAbsolutePath()).build();
        try {
            final int status = PluginCli.main(args, new PluginCli(new Environment(build)) {

            }, terminal);
            assertThat(status, equalTo(expectedStatus));
            outputConsumer.accept(terminal.getOutput());
        } catch (Throwable t) {
            logger.info(terminal.getOutput());
            throw t;
        }
    }
}
