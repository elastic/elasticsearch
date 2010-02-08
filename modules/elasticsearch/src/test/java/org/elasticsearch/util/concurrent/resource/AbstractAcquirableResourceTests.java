/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.util.concurrent.resource;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.util.StopWatch;
import org.elasticsearch.util.lease.Releasable;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

/**
 * @author kimchy
 */
public abstract class AbstractAcquirableResourceTests {

    protected abstract <T extends Releasable> AcquirableResource<T> createInstance(T resource);

    @Test public void testSimple() throws Exception {
        ExecutorService executorService = Executors.newCachedThreadPool();

        final AcquirableResource<Resource> acquirableResource = createInstance(new Resource());

        List<Future> results = new ArrayList<Future>();


        final int cycles = 50;
        final int operationsWithinCycle = 100000;
        final CyclicBarrier barrier1 = new CyclicBarrier(cycles * 2 + 1);
        final CyclicBarrier barrier2 = new CyclicBarrier(cycles * 2 + 1);

        for (int i = 0; i < cycles; i++) {
            results.add(executorService.submit(new Callable() {
                @Override public Object call() throws Exception {
                    barrier1.await();
                    barrier2.await();
                    for (int j = 0; j < operationsWithinCycle; j++) {
                        assertThat(acquirableResource.acquire(), equalTo(true));
                    }
                    return null;
                }
            }));
            results.add(executorService.submit(new Callable() {
                @Override public Object call() throws Exception {
                    barrier1.await();
                    barrier2.await();
                    for (int j = 0; j < operationsWithinCycle; j++) {
                        acquirableResource.release();
                    }
                    return null;
                }
            }));
        }
        barrier1.await();

        StopWatch stopWatch = new StopWatch("Acquirable");
        stopWatch.start();

        barrier2.await();

        for (Future f : results) {
            f.get();
        }

        assertThat(acquirableResource.resource().isReleased(), equalTo(false));
        acquirableResource.markForClose();
        assertThat(acquirableResource.resource().isReleased(), equalTo(true));

        stopWatch.stop();
        System.out.println("Took: " + stopWatch.shortSummary());
    }

    private static class Resource implements Releasable {

        private volatile boolean released = false;

        @Override public boolean release() throws ElasticSearchException {
            released = true;
            return true;
        }

        public boolean isReleased() {
            return released;
        }
    }
}
