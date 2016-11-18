/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.job.logs;

import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.prelert.job.messages.Messages;
import org.hamcrest.core.StringStartsWith;

import java.io.IOException;
import java.nio.file.Path;

public class JobLogsTests extends ESTestCase {

    public void testOperationsNotAllowedWithInvalidPath() throws IOException {
        Path pathOutsideLogsDir = PathUtils.getDefaultFileSystem().getPath("..", "..", "..", "etc");

        Settings settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString()).build();
        Environment env = new Environment(
                settings);

        // delete
        try {
            JobLogs jobLogs = new JobLogs(settings);
            jobLogs.deleteLogs(env, pathOutsideLogsDir.toString());
            fail();
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), StringStartsWith.startsWith("Invalid log file path."));
        }
    }

    public void testSanitizePath_GivenInvalid() {

        Settings settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString()).build();
        Path filePath = PathUtils.getDefaultFileSystem().getPath("/opt", "prelert", "../../etc");
        try {
            Path rootDir = PathUtils.getDefaultFileSystem().getPath("/opt", "prelert");
            new JobLogs(settings).sanitizePath(filePath, rootDir);
            fail();
        } catch (IllegalArgumentException e) {
            assertEquals(Messages.getMessage(Messages.LOGFILE_INVALID_PATH, filePath), e.getMessage());
        }
    }

    public void testSanitizePath() {

        Settings settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString()).build();
        Path filePath = PathUtils.getDefaultFileSystem().getPath("/opt", "prelert", "logs", "logfile.log");
        Path rootDir = PathUtils.getDefaultFileSystem().getPath("/opt", "prelert", "logs");
        Path normalized = new JobLogs(settings).sanitizePath(filePath, rootDir);
        assertEquals(filePath, normalized);

        Path logDir = PathUtils.getDefaultFileSystem().getPath("./logs");
        Path filePathStartingDot = logDir.resolve("farequote").resolve("logfile.log");
        normalized = new JobLogs(settings).sanitizePath(filePathStartingDot, logDir);
        assertEquals(filePathStartingDot.normalize(), normalized);

        Path filePathWithDotDot = PathUtils.getDefaultFileSystem().getPath("/opt", "prelert", "logs", "../logs/logfile.log");
        rootDir = PathUtils.getDefaultFileSystem().getPath("/opt", "prelert", "logs");
        normalized = new JobLogs(settings).sanitizePath(filePathWithDotDot, rootDir);

        assertEquals(filePath, normalized);
    }
}
