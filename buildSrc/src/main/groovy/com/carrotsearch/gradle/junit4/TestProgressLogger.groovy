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
import com.carrotsearch.ant.tasks.junit4.events.TestStartedEvent
import com.carrotsearch.ant.tasks.junit4.events.aggregated.AggregatedQuitEvent
import com.carrotsearch.ant.tasks.junit4.events.aggregated.AggregatedStartEvent
import com.carrotsearch.ant.tasks.junit4.events.aggregated.AggregatedSuiteResultEvent
import com.carrotsearch.ant.tasks.junit4.events.aggregated.AggregatedSuiteStartedEvent
import com.carrotsearch.ant.tasks.junit4.events.aggregated.AggregatedTestResultEvent
import com.carrotsearch.ant.tasks.junit4.events.aggregated.ChildBootstrap
import com.carrotsearch.ant.tasks.junit4.events.aggregated.HeartBeatEvent
import com.carrotsearch.ant.tasks.junit4.listeners.AggregatedEventListener
import org.gradle.internal.logging.progress.ProgressLogger
import org.gradle.internal.logging.progress.ProgressLoggerFactory

import static com.carrotsearch.ant.tasks.junit4.FormattingUtils.formatDurationInSeconds
import static com.carrotsearch.ant.tasks.junit4.events.aggregated.TestStatus.ERROR
import static com.carrotsearch.ant.tasks.junit4.events.aggregated.TestStatus.FAILURE
import static com.carrotsearch.ant.tasks.junit4.events.aggregated.TestStatus.IGNORED
import static com.carrotsearch.ant.tasks.junit4.events.aggregated.TestStatus.IGNORED_ASSUMPTION
import static com.carrotsearch.ant.tasks.junit4.events.aggregated.TestStatus.OK

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
    ProgressLogger parentProgressLogger
    ProgressLogger suiteLogger
    ProgressLogger testLogger
    ProgressLogger[] slaveLoggers
    int totalSuites
    int totalSlaves

    // Counters incremented test completion.
    volatile int suitesCompleted = 0
    volatile int testsCompleted = 0
    volatile int testsFailed = 0
    volatile int testsIgnored = 0

    @Subscribe
    void onStart(AggregatedStartEvent e) throws IOException {
        totalSuites = e.suiteCount
        totalSlaves = e.slaveCount
        parentProgressLogger = factory.newOperation(TestProgressLogger)
        parentProgressLogger.setDescription('Randomized test runner')
        parentProgressLogger.started()

        suiteLogger = factory.newOperation(TestProgressLogger, parentProgressLogger)
        suiteLogger.setDescription('Suite logger')
        suiteLogger.started("Suites: 0/" + totalSuites)
        testLogger = factory.newOperation(TestProgressLogger, parentProgressLogger)
        testLogger.setDescription('Test logger')
        testLogger.started('Tests: completed: 0, failed: 0, ignored: 0')
        slaveLoggers = new ProgressLogger[e.slaveCount]
        for (int i = 0; i < e.slaveCount; ++i) {
            slaveLoggers[i] = factory.newOperation(TestProgressLogger, parentProgressLogger)
            slaveLoggers[i].setDescription("J${i} test logger")
            slaveLoggers[i].started("J${i}: initializing...")
        }
    }

    @Subscribe
    void onChildBootstrap(ChildBootstrap e) throws IOException {
        slaveLoggers[e.getSlave().id].progress("J${e.slave.id}: starting (pid ${e.slave.pidString})")
    }

    @Subscribe
    void onQuit(AggregatedQuitEvent e) throws IOException {
        // if onStart was never called (eg no matching tests), suiteLogger and all the other loggers will be null
        if (suiteLogger != null) {
            suiteLogger.completed()
            testLogger.completed()
            for (ProgressLogger slaveLogger : slaveLoggers) {
                slaveLogger.completed()
            }
            parentProgressLogger.completed()
        }
    }

    @Subscribe
    void onSuiteStart(AggregatedSuiteStartedEvent e) throws IOException {
        String suiteName = simpleName(e.suiteStartedEvent.description.className)
        slaveLoggers[e.slave.id].progress("J${e.slave.id}: ${suiteName} - initializing")
    }

    @Subscribe
    void onSuiteResult(AggregatedSuiteResultEvent e) throws IOException {
        suitesCompleted++
        suiteLogger.progress("Suites: " + suitesCompleted + "/" + totalSuites)
    }

    @Subscribe
    void onTestResult(AggregatedTestResultEvent e) throws IOException {
        String statusMessage
        testsCompleted++
        switch (e.status) {
        case ERROR:
        case FAILURE:
            testsFailed++
            statusMessage = "failed"
            break
        case IGNORED:
        case IGNORED_ASSUMPTION:
            testsIgnored++
            statusMessage = "ignored"
            break
        case OK:
            String time = formatDurationInSeconds(e.executionTime)
            statusMessage = "completed [${time}]"
            break
        default:
            throw new IllegalArgumentException("Unknown test status: [${e.status}]")
        }
        testLogger.progress("Tests: completed: ${testsCompleted}, failed: ${testsFailed}, ignored: ${testsIgnored}")
        String testName = simpleName(e.description.className) + '.' + e.description.methodName
        slaveLoggers[e.slave.id].progress("J${e.slave.id}: ${testName} ${statusMessage}")
    }

    @Subscribe
    void onTestStarted(TestStartedEvent e) throws IOException {
        String testName = simpleName(e.description.className) + '.' + e.description.methodName
        slaveLoggers[e.slave.id].progress("J${e.slave.id}: ${testName} ...")
    }

    @Subscribe
    void onHeartbeat(HeartBeatEvent e) throws IOException {
        String testName = simpleName(e.description.className) + '.' + e.description.methodName
        String time = formatDurationInSeconds(e.getNoEventDuration())
        slaveLoggers[e.slave.id].progress("J${e.slave.id}: ${testName} stalled for ${time}")
    }

    /**
     * Extract a Class#getSimpleName style name from Class#getName style
     * string. We can't just use Class#getSimpleName because junit descriptions
     * don't always set the class field but they always set the className
     * field.
     */
    private static String simpleName(String className) {
        return className.substring(className.lastIndexOf('.') + 1)
    }

    @Override
    void setOuter(JUnit4 junit) {}
}
