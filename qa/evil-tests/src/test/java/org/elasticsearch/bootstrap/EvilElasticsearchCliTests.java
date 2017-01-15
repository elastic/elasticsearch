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

import java.util.Map;

import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.common.SuppressForbidden;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;

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
                (foreground, pidFile, quiet, esSettings) -> {
                    Map<String, String> settings = esSettings.settings().getAsMap();
                    assertThat(settings.size(), equalTo(2));
                    assertThat(settings, hasEntry("path.home", value));
                    assertThat(settings, hasKey("path.logs")); // added by env initialization
                });

        System.clearProperty("es.path.home");
        final String commandLineValue = randomAsciiOfLength(16);
        runTest(
                ExitCodes.OK,
                true,
                output -> {},
                (foreground, pidFile, quiet, esSettings) -> {
                    Map<String, String> settings = esSettings.settings().getAsMap();
                    assertThat(settings.size(), equalTo(2));
                    assertThat(settings, hasEntry("path.home", commandLineValue));
                    assertThat(settings, hasKey("path.logs")); // added by env initialization
                },
                "-Epath.home=" + commandLineValue);

        if (pathHome != null) System.setProperty("es.path.home", pathHome);
        else System.clearProperty("es.path.home");
    }

}
