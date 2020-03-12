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
package org.elasticsearch.cluster.action.index;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.action.index.MappingUpdatedAction.AdjustableSemaphore;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.Mapping;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class MappingUpdatedActionTests extends ESTestCase {

    public void testAdjustableSemaphore() {
        AdjustableSemaphore sem = new AdjustableSemaphore(1, randomBoolean());
        assertEquals(1, sem.availablePermits());
        assertTrue(sem.tryAcquire());
        assertEquals(0, sem.availablePermits());
        assertFalse(sem.tryAcquire());
        assertEquals(0, sem.availablePermits());

        // increase the number of max permits to 2
        sem.setMaxPermits(2);
        assertEquals(1, sem.availablePermits());
        assertTrue(sem.tryAcquire());
        assertEquals(0, sem.availablePermits());

        // release all current permits
        sem.release();
        assertEquals(1, sem.availablePermits());
        sem.release();
        assertEquals(2, sem.availablePermits());

        // reduce number of max permits to 1
        sem.setMaxPermits(1);
        assertEquals(1, sem.availablePermits());
        // set back to 2
        sem.setMaxPermits(2);
        assertEquals(2, sem.availablePermits());

        // take both permits and reduce max permits
        assertTrue(sem.tryAcquire());
        assertTrue(sem.tryAcquire());
        assertEquals(0, sem.availablePermits());
        assertFalse(sem.tryAcquire());
        sem.setMaxPermits(1);
        assertEquals(-1, sem.availablePermits());
        assertFalse(sem.tryAcquire());

        // release one permit
        sem.release();
        assertEquals(0, sem.availablePermits());
        assertFalse(sem.tryAcquire());

        // release second permit
        sem.release();
        assertEquals(1, sem.availablePermits());
        assertTrue(sem.tryAcquire());
    }

    public void testMappingUpdatedActionBlocks() throws Exception {
        List<ActionListener<Void>> inFlightListeners = new CopyOnWriteArrayList<>();
        final MappingUpdatedAction mua = new MappingUpdatedAction(Settings.builder()
            .put(MappingUpdatedAction.INDICES_MAX_IN_FLIGHT_UPDATES_SETTING.getKey(), 1).build(),
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)) {

            @Override
            protected void sendUpdateMapping(Index index, Mapping mappingUpdate, ActionListener<Void> listener) {
                inFlightListeners.add(listener);
            }
        };

        PlainActionFuture<Void> fut1 = new PlainActionFuture<>();
        mua.updateMappingOnMaster(null, null, fut1);
        assertEquals(1, inFlightListeners.size());
        assertEquals(0, mua.blockedThreads());

        PlainActionFuture<Void> fut2 = new PlainActionFuture<>();
        Thread thread = new Thread(() -> {
            mua.updateMappingOnMaster(null, null, fut2); // blocked
        });
        thread.start();
        assertBusy(() -> assertEquals(1, mua.blockedThreads()));

        assertEquals(1, inFlightListeners.size());
        assertFalse(fut1.isDone());
        inFlightListeners.remove(0).onResponse(null);
        assertTrue(fut1.isDone());

        thread.join();
        assertEquals(0, mua.blockedThreads());
        assertEquals(1, inFlightListeners.size());
        assertFalse(fut2.isDone());
        inFlightListeners.remove(0).onResponse(null);
        assertTrue(fut2.isDone());
    }
}
