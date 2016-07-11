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

import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;

public class EvilElasticsearchCliTests extends ESElasticsearchCliTestCase {

    @SuppressForbidden(reason = "manipulates system properties for testing")
    public void testPathHome() throws Exception {
        final String pathHome = System.getProperty("es.path.home");
        final String value = randomAsciiOfLength(16);
        System.setProperty("es.path.home", value);

        runTest(
                ExitCodes.OK,
                true,
                output -> {},
                (foreground, pidFile, esSettings) -> {
                    assertThat(esSettings.size(), equalTo(1));
                    assertThat(esSettings, hasEntry("path.home", value));
                });

        System.clearProperty("es.path.home");
        final String commandLineValue = randomAsciiOfLength(16);
        runTest(
                ExitCodes.OK,
                true,
                output -> {},
                (foreground, pidFile, esSettings) -> {
                    assertThat(esSettings.size(), equalTo(1));
                    assertThat(esSettings, hasEntry("path.home", commandLineValue));
                },
                "-Epath.home=" + commandLineValue);

        if (pathHome != null) System.setProperty("es.path.home", pathHome);
        else System.clearProperty("es.path.home");
    }

}
