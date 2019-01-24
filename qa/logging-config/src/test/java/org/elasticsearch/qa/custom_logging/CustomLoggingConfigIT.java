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

package org.elasticsearch.qa.custom_logging;

import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.test.rest.ESRestTestCase;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * This test verifies that Elasticsearch can startup successfully with a custom logging config using variables introduced in
 * <code>ESJsonLayout</code>
 * The intention is to confirm that users can still run their Elasticsearch instances with previous configurations.
 */
public class CustomLoggingConfigIT extends ESRestTestCase {
    private static final Pattern NODE_STARTED = Pattern.compile(
        ".*node-0 \"cluster.uuid\": \"\\w*\", \"node.id\": \"\\w*\".*started.*");

    public void testSuccessfulStartupWithCustomConfig() throws Exception {
        assertBusy(() -> {
            try (Stream<String> lines = streamLogLines(getLogFile())) {
                assertTrue("Log line indicating successful startup not found\n",
                    lines.anyMatch(line -> isStartupLine(line)));
            }
        });
    }

    private boolean isStartupLine(String line) {
        Matcher matcher = NODE_STARTED.matcher(line);
        return matcher.matches();
    }

    private Stream<String> streamLogLines(Path logFile) {
        return AccessController.doPrivileged((PrivilegedAction<Stream<String>>) () -> {
            try {
                return Files.lines(logFile, StandardCharsets.UTF_8);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    @SuppressForbidden(reason = "PathUtils doesn't have permission to read this file")
    private Path getLogFile() {
        String logFileString = System.getProperty("tests.logfile");
        if (logFileString == null) {
            fail("tests.logfile must be set to run this test. It is automatically "
                + "set by gradle. If you must set it yourself then it should be the absolute path to the "
                + "log file.");
        }
        return Paths.get(logFileString);
    }
}
