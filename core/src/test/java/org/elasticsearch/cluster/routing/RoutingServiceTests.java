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

package org.elasticsearch.cluster.routing;

import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

public class RoutingServiceTests extends ESAllocationTestCase {

    private TestRoutingService routingService;
    private ThreadPool threadPool;

    @After
    public void stopThreadPool() throws Exception {
        super.tearDown();
        if (threadPool != null) {
            threadPool.shutdownNow();
            threadPool = null;
        }
    }

    @Before
    public void createRoutingService() {
        if (threadPool == null) {
            threadPool = new TestThreadPool(getTestClass().getName());
        }
        this.routingService = new TestRoutingService(threadPool);
    }

    public void testReroute() {
        assertThat(routingService.getSchedulesAndClear(), empty());
        routingService.reroute("test");
        assertThat(routingService.getSchedulesAndClear(), contains("test"));
    }

    public void testScheduleReroute() throws Exception {
        assertThat(routingService.getSchedulesAndClear(), empty());
        routingService.scheduleReroute("Schedule rerouting", TimeValue.timeValueMillis(20));
        assertBusy(() -> assertThat(routingService.getSchedulesAndClear(), contains("Schedule rerouting")),
            1, TimeUnit.SECONDS);
    }

    public void testScheduleMultipleTimesCollapseToSingle() throws Exception {
        assertThat(routingService.getSchedulesAndClear(), empty());
        IntStream.rangeClosed(0, randomIntBetween(5, 10)).forEach(n ->
            routingService.scheduleReroute("Schedule-" + n, TimeValue.timeValueMillis(randomIntBetween(200, 300))));

        routingService.scheduleReroute("master", TimeValue.timeValueMillis(randomInt(10)));
        boolean rerouteMoreThanOnce = awaitBusy(() -> routingService.schedules.size() > 1, 2, TimeUnit.SECONDS);
        assertThat(rerouteMoreThanOnce, equalTo(false));
    }

    public void testScheduleOverridePreviousSchedule() throws Exception {
        assertThat(routingService.getSchedulesAndClear(), empty());
        routingService.scheduleReroute("1000ms", TimeValue.timeValueMillis(1000));
        routingService.scheduleReroute("108ms", TimeValue.timeValueMillis(108));
        routingService.scheduleReroute("500ms", TimeValue.timeValueMillis(500));
        routingService.scheduleReroute("100ms", TimeValue.timeValueMillis(100));
        assertBusy(() -> assertTrue(routingService.schedules.contains("100ms")));
        assertBusy(() -> assertFalse(routingService.schedules.contains("108ms")));
    }

    private class TestRoutingService extends RoutingService {

        final BlockingQueue<String> schedules = new LinkedBlockingQueue<>();

        TestRoutingService(ThreadPool threadPool) {
            super(Settings.EMPTY, null, null, threadPool);
        }

        List<String> getSchedulesAndClear() {
            List<String> result = new ArrayList<>();
            schedules.drainTo(result);
            return result;
        }

        @Override
        protected void performReroute(String reason) {
            logger.info("--> performing fake reroute [{}]", reason);
            schedules.add(reason);
        }
    }
}
