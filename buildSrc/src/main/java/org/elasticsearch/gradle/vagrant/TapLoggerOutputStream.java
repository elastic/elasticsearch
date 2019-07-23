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

package org.elasticsearch.gradle.vagrant;

import org.elasticsearch.gradle.LoggingOutputStream;
import org.gradle.api.GradleScriptException;
import org.gradle.api.logging.Logger;
import org.gradle.internal.logging.progress.ProgressLogger;

import java.util.Formatter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Adapts an OutputStream containing output from bats into a ProgressLogger
 * and a Logger. Every test output goes to the ProgressLogger and all failures
 * and non-test output goes to the Logger. That means you can always glance
 * at the result of the last test and the cumulative pass/fail/skip stats and
 * the failures are all logged.
 *
 * There is a Tap4j project but we can't use it because it wants to parse the
 * entire TAP stream at once and won't parse it stream-wise.
 */
public class TapLoggerOutputStream extends LoggingOutputStream {

    private static final Pattern lineRegex =
        Pattern.compile("(?<status>ok|not ok) \\d+(?<skip> # skip (?<skipReason>\\(.+\\))?)? \\[(?<suite>.+)\\] (?<test>.+)");

    private final Logger logger;
    private final ProgressLogger progressLogger;
    private boolean isStarted = false;
    private int testsCompleted = 0;
    private int testsFailed = 0;
    private int testsSkipped = 0;
    private Integer testCount;
    private String countsFormat;

    TapLoggerOutputStream(Logger logger, ProgressLogger progressLogger) {
        this.logger = logger;
        this.progressLogger = progressLogger;
    }

    @Override
    public void logLine(String line) {
        if (isStarted == false) {
            progressLogger.started("started");
            isStarted = true;
        }
        if (testCount == null) {
            try {
                int lastDot = line.lastIndexOf('.');
                testCount = Integer.parseInt(line.substring(lastDot + 1));
                int length = String.valueOf(testCount).length();
                String count = "%0" + length + "d";
                countsFormat = "[" + count +"|" + count + "|" + count + "/" + count + "]";
                return;
            } catch (Exception e) {
                throw new GradleScriptException("Error parsing first line of TAP stream!!", e);
            }
        }
        Matcher m = lineRegex.matcher(line);
        if (m.matches() == false) {
            /* These might be failure report lines or comments or whatever. Its hard
              to tell and it doesn't matter. */
            logger.warn(line);
            return;
        }
        boolean skipped = m.group("skip") != null;
        boolean success = skipped == false && m.group("status").equals("ok");
        String skipReason = m.group("skipReason");
        String suiteName = m.group("suite");
        String testName = m.group("test");

        final String status;
        if (skipped) {
            status = "SKIPPED";
            testsSkipped++;
        } else if (success) {
            status = "     OK";
            testsCompleted++;
        } else {
            status = " FAILED";
            testsFailed++;
        }

        String counts = new Formatter().format(countsFormat, testsCompleted, testsFailed, testsSkipped, testCount).out().toString();
        progressLogger.progress("BATS " + counts + ", " + status + " [" + suiteName + "] " + testName);
        if (success == false) {
            logger.warn(line);
        }
    }

    @Override
    public void close() {
        progressLogger.completed();
    }
}
