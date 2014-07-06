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

package org.elasticsearch.common.collect;

import com.google.common.collect.Iterables;
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;

import static org.hamcrest.Matchers.*;

/**
 */
public class UpdateInPlaceMapTests extends ElasticsearchTestCase {

    @Test
    public void testConcurrentMutator() {
        UpdateInPlaceMap<String, String> map = UpdateInPlaceMap.of(randomIntBetween(0, 500));
        UpdateInPlaceMap<String, String>.Mutator mutator = map.mutator();
        try {
            map.mutator();
            fail("should fail on concurrent mutator");
        } catch (ElasticsearchIllegalStateException e) {
            // all is well!
        }
        mutator.close();
        // now this should work well!
        map.mutator();
    }

    @Test
    public void testImmutableMapSwitchToCHM() {
        int switchSize = randomIntBetween(1, 500);
        UpdateInPlaceMap<String, String> map = UpdateInPlaceMap.of(switchSize);
        int i;
        for (i = 0; i < switchSize; i++) {
            UpdateInPlaceMap<String, String>.Mutator mutator = map.mutator();
            String key = "key" + i;
            String value = "value" + i;
            mutator.put(key, value);
            assertThat(mutator.get(key), equalTo(value));
            assertThat(map.get(key), nullValue());
            mutator.close();
            assertThat(map.get(key), equalTo(value));
        }
        int countAfter = switchSize + randomIntBetween(0, 100);
        for (; i < countAfter; i++) {
            UpdateInPlaceMap<String, String>.Mutator mutator = map.mutator();
            String key = "key" + i;
            String value = "value" + i;
            mutator.put(key, value);
            assertThat(mutator.get(key), equalTo(value));
            assertThat(map.get(key), equalTo(value));
            mutator.close();
            assertThat(map.get(key), equalTo(value));
        }
    }

    @Test
    public void testInitializeWithCHM() {
        UpdateInPlaceMap<String, String> map = UpdateInPlaceMap.of(0);
        UpdateInPlaceMap<String, String>.Mutator mutator = map.mutator();
        mutator.put("key1", "value1");
        assertThat(mutator.get("key1"), equalTo("value1"));
        mutator.put("key2", "value2");
        assertThat(mutator.get("key2"), equalTo("value2"));
    }

    @Test
    public void testConcurrentAccess() throws Exception {
        final int numberOfThreads = scaledRandomIntBetween(1, 10);
        final int switchSize = randomIntBetween(1, 500);
        final CountDownLatch numberOfMutations = new CountDownLatch(scaledRandomIntBetween(300, 1000));

        final UpdateInPlaceMap<String, String> map = UpdateInPlaceMap.of(switchSize);
        final ConcurrentMap<String, String> verifier = ConcurrentCollections.newConcurrentMap();

        Thread[] threads = new Thread[numberOfThreads];
        for (int i = 0; i < numberOfThreads; i++) {
            threads[i] = new Thread(new Runnable() {
                @Override
                public void run() {
                    while (numberOfMutations.getCount() > 0) {
                        try {
                            UpdateInPlaceMap<String, String>.Mutator mutator = map.mutator();
                            String str = Strings.randomBase64UUID();
                            mutator.put(str, str);
                            verifier.put(str, str);
                            mutator.close();
                            numberOfMutations.countDown();
                        } catch (ElasticsearchIllegalStateException e) {
                            // ok, double mutating, continue
                        }
                    }
                }
            }, getClass().getName() + "concurrent_access_i");
            threads[i].setDaemon(true);
        }

        for (Thread thread : threads) {
            thread.start();
        }

        numberOfMutations.await();

        for (Thread thread : threads) {
            thread.join();
        }

        // verify the 2 maps are the same
        assertThat(Iterables.toArray(map.values(), String.class), arrayContainingInAnyOrder(Iterables.toArray(verifier.values(), String.class)));
    }
}
