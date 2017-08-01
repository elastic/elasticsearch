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

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.junit.Before;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.equalTo;

public class RoutingServiceTests extends ESAllocationTestCase {

    private TestRoutingService routingService;

    @Before
    public void createRoutingService() {
        routingService = new TestRoutingService();
    }

    public void testReroute() {
        assertThat(routingService.hasReroutedAndClear(), equalTo(false));
        routingService.reroute("test");
        assertThat(routingService.hasReroutedAndClear(), equalTo(true));
    }

    private class TestRoutingService extends RoutingService {

        private AtomicBoolean rerouted = new AtomicBoolean();

        TestRoutingService() {
            super(Settings.EMPTY, null, null);
        }

        public boolean hasReroutedAndClear() {
            return rerouted.getAndSet(false);
        }

        @Override
        protected void performReroute(String reason) {
            logger.info("--> performing fake reroute [{}]", reason);
            rerouted.set(true);
        }
    }
}
