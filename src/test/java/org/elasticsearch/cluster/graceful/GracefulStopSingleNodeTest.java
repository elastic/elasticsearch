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
        numDataNodes = 1,
        numClientNodes = 0,
        enableRandomBenchNodes = false)
public class GracefulStopSingleNodeTest extends GracefulStopTestBase {

    private void assertSingleNodeGracefulStop() {
        assertThat(gracefulStop.deallocate(), is(true));
        assertThat(deallocators.isDeallocating(), is(false));
    }

    @Test
    public void testNoReallocateCouldNotDeallocate() throws Exception {
        setSettings(false, false, Deallocators.MinAvailability.FULL, "2m");
        createIndex(3, 0, 10);
        assertSingleNodeGracefulStop();
    }

    @Test
    public void testNoReallocateCouldDeallocate() throws Exception {
        setSettings(false, false, Deallocators.MinAvailability.FULL, "2m");
        createIndex(3, 1, 10);
        assertSingleNodeGracefulStop();
    }

    @Test
    public void testReallocateFullCouldNotDeallocate() throws Exception {
        setSettings(false, true, Deallocators.MinAvailability.FULL, "2m");
        createIndex(3, 1, 10);
        assertSingleNodeGracefulStop();
    }

    @Test
    public void testReallocateFullCouldDeallocate() throws Exception {
        setSettings(false, true, Deallocators.MinAvailability.FULL, "2m");
        assertSingleNodeGracefulStop();
    }

    @Test
    public void testNoReallocatePrimaries() throws Exception {
        setSettings(false, false, Deallocators.MinAvailability.PRIMARIES, "2m");
        createIndex(3, 0, 10);
        assertSingleNodeGracefulStop();
    }

    @Test
    public void testReallocatePrimaries() throws Exception {
        setSettings(false, true, Deallocators.MinAvailability.PRIMARIES, "2m");
        createIndex(3, 0, 10);
        assertSingleNodeGracefulStop();
    }

    @Test
    public void testNoReallocateNone() throws Exception {
        setSettings(false, false, Deallocators.MinAvailability.NONE, "2m");
        createIndex(3, 0, 10);
        assertSingleNodeGracefulStop();
    }

    @Test
    public void testReallocateNone() throws Exception {
        setSettings(false, true, Deallocators.MinAvailability.NONE, "2m");
        createIndex(3, 0, 10);
        assertSingleNodeGracefulStop();
    }
}
