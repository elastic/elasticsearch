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

package org.elasticsearch.cluster.graceful;

import org.elasticsearch.cluster.routing.allocation.deallocator.Deallocators;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import static org.hamcrest.core.Is.is;

@ElasticsearchIntegrationTest.ClusterScope(
        scope= ElasticsearchIntegrationTest.Scope.TEST,
        numDataNodes = 3,
        numClientNodes = 0,
        enableRandomBenchNodes = false)
public class GracefulStopTest extends GracefulStopTestBase {

    @Test
    public void testNoReallocateCannotDeallocate() throws Exception {
        setSettings(false, false, Deallocators.MinAvailability.FULL, "2m");
        createIndex(3, 0, 10);

        assertThat(gracefulStop.reallocate(), is(false));
        // cannot deallocate
        assertThat(gracefulStop.deallocate(), is(false));
        assertThat(deallocators.isDeallocating(), is(false));
        assertAllocationSettingsGotReset();
    }

    @Test
    public void testNoReallocate() throws Exception {
        setSettings(false, false, Deallocators.MinAvailability.PRIMARIES, "2m");
        createIndex(3, 1, 10);

        assertThat(gracefulStop.reallocate(), is(false));
        assertThat(gracefulStop.deallocate(), is(true));
        assertThat(deallocators.isDeallocating(), is(false));
        assertAllocationSettingsGotReset();
    }

    @Test
    public void testReallocateMinAvailabilityFull() throws Exception {
        setSettings(false, true, Deallocators.MinAvailability.FULL, "2m");
        createIndex(3, 1, 10);

        assertThat(gracefulStop.reallocate(), is(true));
        assertThat(gracefulStop.deallocate(), is(true));
        assertThat(deallocators.isDeallocating(), is(true)); // still deallocated
        assertAllocationSettingsGotReset();
    }

    @Test
    public void testReallocateMinAvailabilityFullCannotDeallocate() throws Exception {
        setSettings(false, true, Deallocators.MinAvailability.FULL, "2m");
        createIndex(3, 2, 10);

        assertThat(gracefulStop.reallocate(), is(true));
        assertThat(gracefulStop.deallocate(), is(false));
        assertThat(deallocators.isDeallocating(), is(false)); // not deallocating
        assertAllocationSettingsGotReset();
    }

    @Test
    public void testNoReallocateMinAvailabilityFull() throws Exception {
        setSettings(false, false, Deallocators.MinAvailability.FULL, "2m");
        createIndex(3, 1, 10);

        assertThat(gracefulStop.reallocate(), is(false));
        assertThat(gracefulStop.deallocate(), is(false));
        assertThat(deallocators.isDeallocating(), is(false));
        assertAllocationSettingsGotReset();
    }

    @Test
    public void testReallocateMinAvailabilityPrimaries() throws Exception {
        setSettings(false, true, Deallocators.MinAvailability.PRIMARIES, "2m");
        createIndex(30, 0, 10);

        assertThat(gracefulStop.reallocate(), is(true));
        assertThat(gracefulStop.deallocate(), is(true));
        assertThat(deallocators.isDeallocating(), is(true)); // still deallocating
        assertAllocationSettingsGotReset();
    }

    @Test
    public void testReallocateMinAvailabilityNone() throws Exception {
        setSettings(false, true, Deallocators.MinAvailability.NONE, "2m");
        createIndex(30, 2, 10);

        assertThat(gracefulStop.reallocate(), is(true));
        assertThat(gracefulStop.deallocate(), is(true));
        assertThat(deallocators.isDeallocating(), is(false)); // not deallocating
        assertAllocationSettingsGotReset();
    }

    @Test
    public void testTimeout() throws Exception {
        setSettings(false, true, Deallocators.MinAvailability.PRIMARIES, "1ms");
        createIndex(30, 0, 30);

        assertThat(gracefulStop.reallocate(), is(true));
        assertThat(gracefulStop.deallocate(), is(false));
        assertAllocationSettingsGotReset();
    }
}
