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

import org.elasticsearch.cluster.service.ClusterService;
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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RepositoriesStatsCollectorTests extends ESTestCase {

    private TestThreadPool threadPool;
    private ClusterService clusterService;
    private ClusterSettings clusterSettings;


    @Before
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getTestName());
        clusterService = mock(ClusterService.class);
        clusterSettings = new ClusterSettings(Settings.EMPTY, Set.of(RepositoriesStatsCollector.ENABLED, RepositoriesStatsCollector.INTERVAL));
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        terminate(threadPool);
    }

    public void testRepositoriesStatsAreCollectedOnConfiguredInterval() {
        List<Map<String, RepositoryStats>> statsOverTime = List.of(Map.of("repo1", RepositoryStats.EMPTY_STATS),
                                                                   Map.of("repo2", RepositoryStats.EMPTY_STATS));

        Iterator<Map<String, RepositoryStats>> iterator = new Iterator<>() {
            private int i = 0;

            @Override
            public boolean hasNext() {
                return true;
            }

            @Override
            public Map<String, RepositoryStats> next() {
                return statsOverTime.get(i++ % statsOverTime.size());
            }
        };

        TestExporter testExporter = new TestExporter();
        Settings settings = Settings.builder().put(RepositoriesStatsCollector.INTERVAL.getKey(), TimeValue.timeValueSeconds(1)).build();
        RepositoriesStatsCollector repositoriesStatsCollector =
            new RepositoriesStatsCollector(settings, clusterService, iterator::next, threadPool, testExporter);

        repositoriesStatsCollector.start();

        assertThat(testExporter, isCalled(TimeValue.timeValueMillis(1010)));
        assertThat(testExporter.callCount(), is(1));
        assertThat(testExporter, isCalled(TimeValue.timeValueMillis(1010)));
        assertThat(testExporter.callCount(), is(2));
        assertThat(testExporter, isCalled(TimeValue.timeValueMillis(1010)));
        assertThat(testExporter.callCount(), is(3));

        repositoriesStatsCollector.stop();

        assertThat(testExporter, isNotCalled(TimeValue.timeValueMillis(1010)));

        assertThat(testExporter.getCall(0), equalTo(statsOverTime.get(0)));
        assertThat(testExporter.getCall(1), equalTo(statsOverTime.get(1)));
        assertThat(testExporter.getCall(2), equalTo(statsOverTime.get(0)));
    }

    public void testStatsAreNotCollectedAfterDisablingTheCollector() {
        TestExporter testExporter = new TestExporter();
        Settings settings = Settings.builder().put(RepositoriesStatsCollector.INTERVAL.getKey(), TimeValue.timeValueSeconds(1)).build();
        Map<String, RepositoryStats> repoStats = Map.of("repo1", RepositoryStats.EMPTY_STATS);

        RepositoriesStatsCollector repositoriesStatsCollector =
            new RepositoriesStatsCollector(settings, clusterService, () -> repoStats, threadPool, testExporter);

        repositoriesStatsCollector.start();
        assertThat(testExporter, isCalled(TimeValue.timeValueMillis(1010)));
        assertThat(testExporter.callCount(), is(1));
        assertThat(testExporter, isCalled(TimeValue.timeValueMillis(1010)));
        assertThat(testExporter.callCount(), is(2));
        repositoriesStatsCollector.setEnabled(false);

        assertThat(testExporter, isNotCalled(TimeValue.timeValueMillis(1010)));

        repositoriesStatsCollector.stop();
        assertThat(testExporter.getCall(0), equalTo(repoStats));
        assertThat(testExporter.getCall(1), equalTo(repoStats));
    }

    public void testIntervalUpdate() {
        TestExporter testExporter = new TestExporter();

        Settings settings = Settings.builder().put(RepositoriesStatsCollector.INTERVAL.getKey(), TimeValue.timeValueSeconds(1)).build();
        Map<String, RepositoryStats> repoStats = Map.of("repo1", RepositoryStats.EMPTY_STATS);
        RepositoriesStatsCollector repositoriesStatsCollector =
            new RepositoriesStatsCollector(settings, clusterService, () -> repoStats, threadPool, testExporter);

        repositoriesStatsCollector.start();

        assertThat(testExporter, isCalled(TimeValue.timeValueMillis(1010)));

        repositoriesStatsCollector.setInterval(TimeValue.timeValueMillis(500));

        assertThat(testExporter, isCalled(TimeValue.timeValueMillis(550)));

        repositoriesStatsCollector.stop();

        assertThat(testExporter, isNotCalled(TimeValue.timeValueMillis(500)));

        assertThat(testExporter.callCount(), is(2));
        assertThat(testExporter.getCall(0), equalTo(repoStats));
        assertThat(testExporter.getCall(1), equalTo(repoStats));
    }

    Matcher<TestExporter> isCalled(TimeValue timeout) {
        return new BaseMatcher<>() {
            @Override
            public boolean matches(Object actual) {
                try {
                    TestExporter testExporter = (TestExporter) actual;
                    return testExporter.waitUntilCalled(timeout);
                } catch (Exception e) {
                    throw new AssertionError("describeTo()");
                }
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("The exporter wasn't called after " + timeout);
            }
        };
    }

    Matcher<TestExporter> isNotCalled(TimeValue timeout) {
        return not(isCalled(timeout));
    }

    private static class TestExporter implements RepositoryStatsExporter {
        private final Semaphore semaphore = new Semaphore(0);
        private final List<Map<String, RepositoryStats>> exporterCalls = new ArrayList<>();

        @Override
        public void export(Map<String, RepositoryStats> repositoriesStats) {
            exporterCalls.add(repositoriesStats);
            semaphore.release();
        }

        int callCount() {
            return exporterCalls.size();
        }

        Map<String, RepositoryStats> getCall(int i) {
            return exporterCalls.get(i);
        }

        boolean waitUntilCalled(TimeValue timeValue) throws Exception {
            return semaphore.tryAcquire(timeValue.millis(), TimeUnit.MILLISECONDS);
        }
    }
}
