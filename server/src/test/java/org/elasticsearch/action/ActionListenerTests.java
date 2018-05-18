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
package org.elasticsearch.action;

import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class ActionListenerTests extends ESTestCase {

    public void testWrap() {
        AtomicReference<Boolean> reference = new AtomicReference<>();
        AtomicReference<Exception> exReference = new AtomicReference<>();

        CheckedConsumer<Boolean, ? extends Exception> handler = (o) -> {
            if (Boolean.FALSE.equals(o)) {
                throw new IllegalArgumentException("must not be false");
            }
            reference.set(o);
        };
        ActionListener<Boolean> wrap = ActionListener.wrap(handler, exReference::set);
        wrap.onResponse(Boolean.FALSE);
        assertNull(reference.get());
        assertNotNull(exReference.get());
        assertEquals("must not be false", exReference.get().getMessage());
        exReference.set(null);

        wrap.onResponse(Boolean.TRUE);
        assertTrue(reference.get());
        assertNull(exReference.get());
    }

    public void testOnResponse() {
        final int numListeners = randomIntBetween(1, 20);
        List<AtomicReference<Boolean>> refList = new ArrayList<>();
        List<AtomicReference<Exception>> excList = new ArrayList<>();
        List<ActionListener<Boolean>> listeners = new ArrayList<>();
        List<Boolean> failOnTrue = new ArrayList<>();
        AtomicInteger exceptionCounter = new AtomicInteger(0);
        for (int i = 0; i < numListeners; i++) {
            boolean doFailOnTrue = rarely();
            failOnTrue.add(doFailOnTrue);
            AtomicReference<Boolean> reference = new AtomicReference<>();
            AtomicReference<Exception> exReference = new AtomicReference<>();
            refList.add(reference);
            excList.add(exReference);
            CheckedConsumer<Boolean, ? extends Exception> handler = (o) -> {
                if (Boolean.FALSE.equals(o)) {
                    throw new IllegalArgumentException("must not be false " + exceptionCounter.getAndIncrement());
                }
                if (doFailOnTrue) {
                    throw new IllegalStateException("must not be true");
                }
                reference.set(o);
            };
            listeners.add(ActionListener.wrap(handler, exReference::set));
        }

        ActionListener.onResponse(listeners, Boolean.TRUE);
        for (int i = 0; i < numListeners; i++) {
            if (failOnTrue.get(i) == false) {
                assertTrue("listener index " + i, refList.get(i).get());
                refList.get(i).set(null);
            } else {
                assertNull("listener index " + i, refList.get(i).get());
            }

        }

        for (int i = 0; i < numListeners; i++) {
            if (failOnTrue.get(i) == false) {
                assertNull("listener index " + i, excList.get(i).get());
            } else {
                assertEquals("listener index " + i, "must not be true", excList.get(i).get().getMessage());
            }
        }

        ActionListener.onResponse(listeners, Boolean.FALSE);
        for (int i = 0; i < numListeners; i++) {
            assertNull("listener index " + i, refList.get(i).get());
        }

        assertEquals(numListeners, exceptionCounter.get());
        for (int i = 0; i < numListeners; i++) {
            assertNotNull(excList.get(i).get());
            assertEquals("listener index " + i, "must not be false " + i, excList.get(i).get().getMessage());
        }
    }

    public void testOnFailure() {
        final int numListeners = randomIntBetween(1, 20);
        List<AtomicReference<Boolean>> refList = new ArrayList<>();
        List<AtomicReference<Exception>> excList = new ArrayList<>();
        List<ActionListener<Boolean>> listeners = new ArrayList<>();

        final int listenerToFail = randomBoolean() ? -1 : randomIntBetween(0, numListeners-1);
        for (int i = 0; i < numListeners; i++) {
            AtomicReference<Boolean> reference = new AtomicReference<>();
            AtomicReference<Exception> exReference = new AtomicReference<>();
            refList.add(reference);
            excList.add(exReference);
            boolean fail = i == listenerToFail;
            CheckedConsumer<Boolean, ? extends Exception> handler = (o) -> {
                reference.set(o);
            };
            listeners.add(ActionListener.wrap(handler, (e) -> {
                exReference.set(e);
                if (fail) {
                    throw new RuntimeException("double boom");
                }
            }));
        }

        try {
            ActionListener.onFailure(listeners, new Exception("booom"));
            assertTrue("unexpected succces listener to fail: " + listenerToFail, listenerToFail == -1);
        } catch (RuntimeException ex) {
            assertTrue("listener to fail: " + listenerToFail, listenerToFail >= 0);
            assertNotNull(ex.getCause());
            assertEquals("double boom", ex.getCause().getMessage());
        }

        for (int i = 0; i < numListeners; i++) {
            assertNull("listener index " + i, refList.get(i).get());
        }

        for (int i = 0; i < numListeners; i++) {
            assertEquals("listener index " + i, "booom", excList.get(i).get().getMessage());
        }
    }
}
