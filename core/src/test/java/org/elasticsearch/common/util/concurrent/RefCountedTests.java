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
package org.elasticsearch.common.util.concurrent;

import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class RefCountedTests extends ESTestCase {
    public void testRefCount() throws IOException {
        MyRefCounted counted = new MyRefCounted();

        int incs = randomIntBetween(1, 100);
        for (int i = 0; i < incs; i++) {
            if (randomBoolean()) {
                counted.incRef();
            } else {
                assertTrue(counted.tryIncRef());
            }
            counted.ensureOpen();
        }

        for (int i = 0; i < incs; i++) {
            counted.decRef();
            counted.ensureOpen();
        }

        counted.incRef();
        counted.decRef();
        for (int i = 0; i < incs; i++) {
            if (randomBoolean()) {
                counted.incRef();
            } else {
                assertTrue(counted.tryIncRef());
            }
            counted.ensureOpen();
        }

        for (int i = 0; i < incs; i++) {
            counted.decRef();
            counted.ensureOpen();
        }

        counted.decRef();
        assertFalse(counted.tryIncRef());
        try {
            counted.incRef();
            fail(" expected exception");
        } catch (AlreadyClosedException ex) {
            assertThat(ex.getMessage(), equalTo("test is already closed can't increment refCount current count [0]"));
        }

        try {
            counted.ensureOpen();
            fail(" expected exception");
        } catch (AlreadyClosedException ex) {
            assertThat(ex.getMessage(), equalTo("closed"));
        }
    }

    public void testMultiThreaded() throws InterruptedException {
        final MyRefCounted counted = new MyRefCounted();
        Thread[] threads = new Thread[randomIntBetween(2, 5)];
        final CountDownLatch latch = new CountDownLatch(1);
        final CopyOnWriteArrayList<Exception> exceptions = new CopyOnWriteArrayList<>();
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread() {
                @Override
                public void run() {
                    try {
                        latch.await();
                        for (int j = 0; j < 10000; j++) {
                            counted.incRef();
                            try {
                                counted.ensureOpen();
                            } finally {
                                counted.decRef();
                            }
                        }
                    } catch (Exception e) {
                        exceptions.add(e);
                    }
                }
            };
            threads[i].start();
        }
        latch.countDown();
        for (int i = 0; i < threads.length; i++) {
            threads[i].join();
        }
        counted.decRef();
        try {
            counted.ensureOpen();
            fail("expected to be closed");
        } catch (AlreadyClosedException ex) {
            assertThat(ex.getMessage(), equalTo("closed"));
        }
        assertThat(counted.refCount(), is(0));
        assertThat(exceptions, Matchers.emptyIterable());

    }

    private final class MyRefCounted extends AbstractRefCounted {

        private final AtomicBoolean closed = new AtomicBoolean(false);

        MyRefCounted() {
            super("test");
        }

        @Override
        protected void closeInternal() {
            this.closed.set(true);
        }

        public void ensureOpen() {
            if (closed.get()) {
                assert this.refCount() == 0;
                throw new AlreadyClosedException("closed");
            }
        }
    }
}
