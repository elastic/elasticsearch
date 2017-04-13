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

package com.carrotsearch.gradle.junit4

import com.carrotsearch.ant.tasks.junit4.JUnit4
import com.carrotsearch.ant.tasks.junit4.dependencies.com.google.common.eventbus.Subscribe
import com.carrotsearch.ant.tasks.junit4.events.aggregated.AggregatedStartEvent
import com.carrotsearch.ant.tasks.junit4.events.aggregated.AggregatedSuiteResultEvent
import com.carrotsearch.ant.tasks.junit4.events.aggregated.AggregatedTestResultEvent
import com.carrotsearch.ant.tasks.junit4.listeners.AggregatedEventListener
import org.gradle.internal.logging.progress.ProgressLogger
import org.gradle.internal.logging.progress.ProgressLoggerFactory

import static com.carrotsearch.ant.tasks.junit4.FormattingUtils.formatDurationInSeconds
import static com.carrotsearch.ant.tasks.junit4.events.aggregated.TestStatus.ERROR
import static com.carrotsearch.ant.tasks.junit4.events.aggregated.TestStatus.FAILURE
import static com.carrotsearch.ant.tasks.junit4.events.aggregated.TestStatus.IGNORED
import static com.carrotsearch.ant.tasks.junit4.events.aggregated.TestStatus.IGNORED_ASSUMPTION
import static com.carrotsearch.ant.tasks.junit4.events.aggregated.TestStatus.OK
import static java.lang.Math.max

/**
 * Adapts junit4's event listeners into gradle's ProgressLogger. Note that
 * junit4 guarantees (via guava) that methods on this class won't be called by
 * multiple threads simultaneously which is helpful in making it simpler.
 *
 * Every time a test finishes this class will update the logger. It will log
 * the last finished test method on the logger line until the first suite
 * finishes. Once the first suite finishes it always logs the last finished
 * suite. This means that in test runs with a single suite the logger will be
 * updated with the test name the whole time which is useful because these runs
 * usually have longer individual tests. For test runs with lots of suites the
 * majority of the time is spent showing the last suite that finished which is
 * more useful for those test runs because test methods there tend to be very
 * quick.
 */
class TestProgressLogger implements AggregatedEventListener {
    /** Factory to build a progress logger when testing starts */
    ProgressLoggerFactory factory
    ProgressLogger progressLogger
    int totalSuites
    int totalSlaves

    // sprintf formats used to align the integers we print
    String suitesFormat
    String slavesFormat
    String testsFormat

    // Counters incremented test completion.
    volatile int suitesCompleted = 0
    volatile int testsCompleted = 0
    volatile int testsFailed = 0
    volatile int testsIgnored = 0

    // Information about the last, most interesting event.
    volatile String eventDescription
    volatile int eventSlave
    volatile long eventExecutionTime

    /** Have we finished a whole suite yet? */
    volatile boolean suiteFinished = false
    /* Note that we probably overuse volatile here but it isn't hurting us and
       lets us move things around without worrying about breaking things. */

    @Subscribe
    void onStart(AggregatedStartEvent e) throws IOException {
        totalSuites = e.suiteCount
        totalSlaves = e.slaveCount
        progressLogger = factory.newOperation(TestProgressLogger)
        progressLogger.setDescription('Randomized test runner')
        progressLogger.started()
        progressLogger.progress(
            "Starting JUnit4 for ${totalSuites} suites on ${totalSlaves} jvms")

        suitesFormat = "%0${widthForTotal(totalSuites)}d"
        slavesFormat = "%-${widthForTotal(totalSlaves)}s"
        /* Just guess the number of tests because we can't figure it out from
          here and it isn't worth doing anything fancy to prevent the console
          from jumping around a little. 200 is a pretty wild guess for the
          minimum but it makes REST tests output sanely. */
        int totalNumberOfTestsGuess = max(200, totalSuites * 10)
        testsFormat = "%0${widthForTotal(totalNumberOfTestsGuess)}d"
    }

    @Subscribe
    void onTestResult(AggregatedTestResultEvent e) throws IOException {
        testsCompleted++
        switch (e.status) {
        case ERROR:
        case FAILURE:
            testsFailed++
            break
        case IGNORED:
        case IGNORED_ASSUMPTION:
            testsIgnored++
            break
        case OK:
            break
        default:
            throw new IllegalArgumentException(
                "Unknown test status: [${e.status}]")
        }
        if (!suiteFinished) {
            updateEventInfo(e)
        }

        log()
    }

    @Subscribe
    void onSuiteResult(AggregatedSuiteResultEvent e) throws IOException {
        suitesCompleted++
        suiteFinished = true
        updateEventInfo(e)
        log()
    }

    /**
     * Update the suite information with a junit4 event.
     */
    private void updateEventInfo(Object e) {
        eventDescription = simpleName(e.description.className)
        if (e.description.methodName != null) {
            eventDescription += "#${e.description.methodName}"
        }
        eventSlave = e.slave.id
        eventExecutionTime = e.executionTime
    }

    /**
     * Extract a Class#getSimpleName style name from Class#getName style
     * string. We can't just use Class#getSimpleName because junit descriptions
     * don't alway s set the class field but they always set the className
     * field.
     */
    private static String simpleName(String className) {
        return className.substring(className.lastIndexOf('.') + 1)
    }

    private void log() {
        /* Remember that instances of this class are only ever active on one
          thread at a time so there really aren't race conditions here. It'd be
          OK if there were because they'd only display an overcount
          temporarily. */
        String log = ''
        if (totalSuites > 1) {
            /* Skip printing the suites to save space when there is only a
              single suite. This is nice because when there is only a single
              suite we log the method name and those can be long. */
            log += sprintf("Suites [${suitesFormat}/${suitesFormat}], ",
                [suitesCompleted, totalSuites])
        }
        log += sprintf("Tests [${testsFormat}|%d|%d], ",
            [testsCompleted, testsFailed, testsIgnored])
        log += "in ${formatDurationInSeconds(eventExecutionTime)} "
        if (totalSlaves > 1) {
            /* Skip printing the slaves if there is only one of them. This is
              nice because when there is only a single slave there is often
              only a single suite and we could use the extra space to log the
              test method names. */
            log += "J${sprintf(slavesFormat, eventSlave)} "
        }
        log += "completed ${eventDescription}"
        progressLogger.progress(log)
    }

    private static int widthForTotal(int total) {
        return ((total - 1) as String).length()
    }

    @Override
    void setOuter(JUnit4 junit) {}
}
