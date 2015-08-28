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

package org.elasticsearch.devtools.randomizedtesting

import com.carrotsearch.ant.tasks.junit4.JUnit4
import com.carrotsearch.ant.tasks.junit4.dependencies.com.google.common.eventbus.Subscribe
import com.carrotsearch.ant.tasks.junit4.events.aggregated.AggregatedStartEvent
import com.carrotsearch.ant.tasks.junit4.events.aggregated.AggregatedSuiteResultEvent
import com.carrotsearch.ant.tasks.junit4.listeners.AggregatedEventListener
import org.gradle.logging.ProgressLogger
import org.gradle.logging.ProgressLoggerFactory
import org.junit.runner.Description

import java.util.concurrent.atomic.AtomicInteger

class JUnit4ProgressLogger implements AggregatedEventListener {

    /** Factory to build a progress logger when testing starts */
    ProgressLoggerFactory factory
    ProgressLogger progressLogger
    int totalSuites;
    AtomicInteger suitesCompleted = new AtomicInteger();

    @Subscribe
    public void onStart(AggregatedStartEvent e) throws IOException {
        totalSuites = e.getSuiteCount();
        progressLogger = factory.newOperation(JUnit4ProgressLogger)
        progressLogger.setDescription('Randomized test runner')
        progressLogger.started()
        progressLogger.progress('Starting JUnit4 with ' + e.getSlaveCount() + ' jvms')
    }

    @Subscribe
    public void onSuiteResult(AggregatedSuiteResultEvent e) throws IOException {
        final int completed = suitesCompleted.incrementAndGet();
        Description description = e.getDescription()
        String suiteName = description.getDisplayName();
        suiteName = suiteName.substring(suiteName.lastIndexOf('.') + 1);
        progressLogger.progress('Completed [' + completed + '/' + totalSuites + '] ' + suiteName)
    }

    @Override
    public void setOuter(JUnit4 junit) {}
}
