/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.repositories;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class RepositoriesStatsCollectorTests extends ESTestCase {

    private TestThreadPool threadPool;
    private ClusterSettings clusterSettings;

    final Map<String, RepositoryStats> repoStats = Map.of("repo1", RepositoryStats.EMPTY_STATS);

    @Before
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getTestName());
        clusterSettings = new ClusterSettings(Settings.EMPTY, Set.of(RepositoriesStatsCollector.INTERVAL));
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        terminate(threadPool);
    }

    public void testRepositoriesStatsAreCollectedOnConfiguredInterval() {
        Settings settings = Settings.builder()
            .put(RepositoriesStatsCollector.INTERVAL.getKey(), TimeValue.timeValueSeconds(1))
            .build();

        TestExporter testExporter = new TestExporter();
        RepositoriesStatsCollector repositoriesStatsCollector =
            new RepositoriesStatsCollector(settings, clusterSettings, () -> repoStats, threadPool, testExporter);

        repositoriesStatsCollector.start();

        assertThat(testExporter, isCalled(TimeValue.timeValueSeconds(1)));
        assertThat(testExporter.callCount(), is(1));

        assertThat(testExporter, isCalled(TimeValue.timeValueSeconds(1)));
        assertThat(testExporter.callCount(), is(2));

        assertThat(testExporter, isCalled(TimeValue.timeValueSeconds(1)));
        assertThat(testExporter.callCount(), is(3));

        repositoriesStatsCollector.stop();

        assertThat(testExporter, isNotCalled(TimeValue.timeValueSeconds(1)));

        assertThat(testExporter.getCall(0), equalTo(repoStats));
        assertThat(testExporter.getCall(1), equalTo(repoStats));
        assertThat(testExporter.getCall(2), equalTo(repoStats));
    }

    public void testStatsAreNotCollectedAfterDisablingTheCollector() {
        Settings settings = Settings.builder()
            .put(RepositoriesStatsCollector.INTERVAL.getKey(), TimeValue.timeValueSeconds(1))
            .build();

        TestExporter testExporter = new TestExporter();
        RepositoriesStatsCollector repositoriesStatsCollector =
            new RepositoriesStatsCollector(settings, clusterSettings, () -> repoStats, threadPool, testExporter);

        repositoriesStatsCollector.start();

        assertThat(testExporter, isCalled(TimeValue.timeValueSeconds(1)));
        assertThat(testExporter.callCount(), is(1));
        assertThat(testExporter, isCalled(TimeValue.timeValueSeconds(1)));
        assertThat(testExporter.callCount(), is(2));

        repositoriesStatsCollector.setInterval(TimeValue.MINUS_ONE);

        assertThat(testExporter, isNotCalled(TimeValue.timeValueSeconds(1)));

        repositoriesStatsCollector.stop();
        assertThat(testExporter.getCall(0), equalTo(repoStats));
        assertThat(testExporter.getCall(1), equalTo(repoStats));
    }

    public void testIntervalUpdate() {
        Settings settings = Settings.builder()
                                    .put(RepositoriesStatsCollector.INTERVAL.getKey(), TimeValue.timeValueSeconds(1))
                                    .build();

        TestExporter testExporter = new TestExporter();
        RepositoriesStatsCollector repositoriesStatsCollector =
            new RepositoriesStatsCollector(settings, clusterSettings, () -> repoStats, threadPool, testExporter);

        repositoriesStatsCollector.start();

        assertThat(testExporter, isCalled(TimeValue.timeValueSeconds(1)));

        repositoriesStatsCollector.setInterval(TimeValue.timeValueMillis(500));

        assertThat(testExporter, isCalled(TimeValue.timeValueMillis(500)));

        repositoriesStatsCollector.stop();

        assertThat(testExporter, isNotCalled(TimeValue.timeValueMillis(500)));

        assertThat(testExporter.callCount(), is(2));
        assertThat(testExporter.getCall(0), equalTo(repoStats));
        assertThat(testExporter.getCall(1), equalTo(repoStats));
    }

    public void testStatsAreNotCollectedWhenDisabled() {
        Settings settings = Settings.builder()
            .put(RepositoriesStatsCollector.INTERVAL.getKey(), TimeValue.MINUS_ONE)
            .build();

        TestExporter testExporter = new TestExporter();
        RepositoriesStatsCollector repositoriesStatsCollector =
            new RepositoriesStatsCollector(settings, clusterSettings, () -> repoStats, threadPool, testExporter);

        repositoriesStatsCollector.start();

        assertThat(testExporter, isNotCalled(TimeValue.timeValueSeconds(2)));

        repositoriesStatsCollector.stop();

        assertThat(testExporter, isNotCalled(TimeValue.timeValueSeconds(1)));

        assertThat(testExporter.callCount(), is(0));
    }

    public void testUpdateIntervalIsIgnoredAfterStop() {
        Settings settings = Settings.builder()
            .put(RepositoriesStatsCollector.INTERVAL.getKey(), TimeValue.timeValueSeconds(1))
            .build();

        TestExporter testExporter = new TestExporter();
        RepositoriesStatsCollector repositoriesStatsCollector =
            new RepositoriesStatsCollector(settings, clusterSettings, () -> repoStats, threadPool, testExporter);

        repositoriesStatsCollector.start();

        assertThat(testExporter, isCalled(TimeValue.timeValueSeconds(1)));

        repositoriesStatsCollector.stop();

        repositoriesStatsCollector.setInterval(TimeValue.timeValueMillis(100));

        assertThat(testExporter, isNotCalled(TimeValue.timeValueMillis(200)));

        assertThat(testExporter.callCount(), is(1));
    }

    Matcher<TestExporter> isNotCalled(TimeValue timeout) {
        return not(isCalled(timeout));
    }

    Matcher<TestExporter> isCalled(TimeValue timeout) {
        return new BaseMatcher<>() {
            @Override
            public boolean matches(Object actual) {
                try {
                    TestExporter testExporter = (TestExporter) actual;
                    return testExporter.waitUntilCalled(timeout);
                } catch (Exception e) {
                    throw new AssertionError("Unable to wait for test exporter to be called", e);
                }
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("The exporter wasn't called after " + timeout);
            }
        };
    }

    private static class TestExporter implements Consumer<Map<String, RepositoryStats>> {
        private final Semaphore semaphore = new Semaphore(0);
        private final List<Map<String, RepositoryStats>> exporterCalls = new ArrayList<>();

        @Override
        public void accept(Map<String, RepositoryStats> repositoriesStats) {
            exporterCalls.add(repositoriesStats);
            semaphore.release();
        }

        int callCount() {
            return exporterCalls.size();
        }

        Map<String, RepositoryStats> getCall(int nth) {
            return exporterCalls.get(nth);
        }

        boolean waitUntilCalled(TimeValue timeout) throws Exception {
            long timeoutInMillis = timeout.millis();
            // Add room for scheduling time
            timeoutInMillis += timeout.millis() / 20;
            return semaphore.tryAcquire(timeoutInMillis, TimeUnit.MILLISECONDS);
        }
    }
}
