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
package org.elasticsearch.gradle.vagrant

import com.carrotsearch.gradle.junit4.LoggingOutputStream
import groovy.transform.PackageScope
import org.gradle.api.GradleScriptException
import org.gradle.api.logging.Logger
import org.gradle.logging.ProgressLogger
import org.gradle.logging.ProgressLoggerFactory

import java.util.regex.Matcher

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
    private final ProgressLogger progressLogger
    private boolean isStarted = false
    private final Logger logger
    private int testsCompleted = 0
    private int testsFailed = 0
    private int testsSkipped = 0
    private Integer testCount
    private String countsFormat

    TapLoggerOutputStream(Map args) {
        logger = args.logger
        progressLogger = args.factory.newOperation(VagrantLoggerOutputStream)
        progressLogger.setDescription("TAP output for `${args.command}`")
    }

    @Override
    public void flush() {
        if (isStarted == false) {
            progressLogger.started()
            isStarted = true
        }
        if (end == start) return
        line(new String(buffer, start, end - start))
        start = end
    }

    void line(String line) {
        // System.out.print "===> $line\n"
        if (testCount == null) {
            try {
                testCount = line.split('\\.').last().toInteger()
                def length = (testCount as String).length()
                countsFormat = "%0${length}d"
                countsFormat = "[$countsFormat|$countsFormat|$countsFormat/$countsFormat]"
                return
            } catch (Exception e) {
                throw new GradleScriptException(
                        'Error parsing first line of TAP stream!!', e)
            }
        }
        Matcher m = line =~ /(?<status>ok|not ok) \d+(?<skip> # skip (?<skipReason>\(.+\))?)? \[(?<suite>.+)\] (?<test>.+)/
        if (!m.matches()) {
            /* These might be failure report lines or comments or whatever. Its hard
              to tell and it doesn't matter. */
            logger.warn(line)
            return
        }
        boolean skipped = m.group('skip') != null
        boolean success = !skipped && m.group('status') == 'ok'
        String skipReason = m.group('skipReason')
        String suiteName = m.group('suite')
        String testName = m.group('test')

        String status
        if (skipped) {
            status = "SKIPPED"
            testsSkipped++
        } else if (success) {
            status = "     OK"
            testsCompleted++
        } else {
            status = " FAILED"
            testsFailed++
        }

        String counts = sprintf(countsFormat,
                [testsCompleted, testsFailed, testsSkipped, testCount])
        progressLogger.progress("Tests $counts, $status [$suiteName] $testName")
        if (!success) {
            logger.warn(line)
        }
    }
}
