/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.bootstrap;


import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.common.settings.Settings;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;

public class EvilElasticsearchCliTests extends ESElasticsearchCliTestCase {

    @SuppressForbidden(reason = "manipulates system properties for testing")
    public void testPathHome() throws Exception {
        final String pathHome = System.getProperty("es.path.home");
        final String value = randomAlphaOfLength(16);
        System.setProperty("es.path.home", value);

        runTest(
                ExitCodes.OK,
                true,
                (output, error) -> {},
                (foreground, pidFile, quiet, esSettings) -> {
                    Settings settings = esSettings.settings();
                    assertThat(settings.keySet(), hasSize(2));
                    assertThat(
                        settings.get("path.home"),
                        equalTo(PathUtils.get(System.getProperty("user.dir")).resolve(value).toString()));
                    assertThat(settings.keySet(), hasItem("path.logs")); // added by env initialization
                });

        System.clearProperty("es.path.home");
        final String commandLineValue = randomAlphaOfLength(16);
        runTest(
                ExitCodes.OK,
                true,
                (output, error) -> {},
                (foreground, pidFile, quiet, esSettings) -> {
                    Settings settings = esSettings.settings();
                    assertThat(settings.keySet(), hasSize(2));
                    assertThat(
                        settings.get("path.home"),
                        equalTo(PathUtils.get(System.getProperty("user.dir")).resolve(commandLineValue).toString()));
                    assertThat(settings.keySet(), hasItem("path.logs")); // added by env initialization
                },
                "-Epath.home=" + commandLineValue);

        if (pathHome != null) System.setProperty("es.path.home", pathHome);
        else System.clearProperty("es.path.home");
    }

}
