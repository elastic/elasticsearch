/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.unconfigured_node_name;

import org.elasticsearch.common.logging.JsonLogsIntegTestCase;
import org.hamcrest.Matcher;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.AccessController;
import java.security.PrivilegedAction;

import static org.hamcrest.Matchers.equalTo;

public class JsonLogsFormatAndParseIT extends JsonLogsIntegTestCase {
    private static final String OS_NAME = System.getProperty("os.name");
    private static final boolean WINDOWS = OS_NAME.startsWith("Windows");

    // These match the values defined in org.elasticsearch.gradle.testclusters.ElasticsearchNode
    private static final String COMPUTERNAME = "WindowsComputername";
    private static final String HOSTNAME = "LinuxDarwinHostname";

    @Override
    protected Matcher<String> nodeNameMatcher() {
        if (WINDOWS) {
            return equalTo(COMPUTERNAME);
        }
        return equalTo(HOSTNAME);
    }

    @Override
    protected BufferedReader openReader(Path logFile) {
        return AccessController.doPrivileged((PrivilegedAction<BufferedReader>) () -> {
            try {
                return Files.newBufferedReader(logFile, StandardCharsets.UTF_8);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }
}
