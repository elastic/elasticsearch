/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.rest;

import org.elasticsearch.common.logging.JsonLogsIntegTestCase;
import org.hamcrest.Matcher;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.AccessController;
import java.security.PrivilegedAction;

import static org.hamcrest.Matchers.is;

public class JsonLogsFormatAndParseIT extends JsonLogsIntegTestCase {
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
                return Files.newBufferedReader(logFile, StandardCharsets.UTF_8);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }
}
