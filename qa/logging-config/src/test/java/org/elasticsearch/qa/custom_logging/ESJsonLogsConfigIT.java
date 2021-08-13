/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.qa.custom_logging;

import org.elasticsearch.common.logging.JsonLogLine;
import org.elasticsearch.common.logging.JsonLogsIntegTestCase;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.hamcrest.Matcher;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.AccessController;
import java.security.PrivilegedAction;

import static org.hamcrest.Matchers.is;

/**
 * Test to verify ES JSON log format. Used in ES v7. Some users might decide to keep that format.
 */
public class ESJsonLogsConfigIT extends JsonLogsIntegTestCase {
    @Override
    protected Matcher<String> nodeNameMatcher() {
        return is("integTest-0");
    }

    @Override
    protected BufferedReader openReader(Path logFile) {
        assumeFalse("Skipping test because it is being run against an external cluster.",
            logFile.getFileName().toString().equals("--external--"));

        return AccessController.doPrivileged((PrivilegedAction<BufferedReader>) () -> {
            try {
                String temp = Files.readString(logFile);

                return Files.newBufferedReader(logFile, StandardCharsets.UTF_8);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    protected String getLogFileName() {
        return System.getProperty("tests.jsonLogfile");
    }

    @Override
    protected ObjectParser<JsonLogLine, Void> getParser() {
        return JsonLogLine.ES_LOG_LINE;
    }
}
