/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.component;

import org.elasticsearch.test.ESTestCase;

import java.util.concurrent.atomic.AtomicInteger;

public class AbstractLifecycleComponentTests extends ESTestCase {

    public void testNormalLifecyle() {

        final var stage = new AtomicInteger();

        final var component = new AbstractLifecycleComponent() {
            @Override
            protected void doStart() {
                assertTrue(stage.compareAndSet(0, 1));
            }

            @Override
            protected void doStop() {
                assertTrue(stage.compareAndSet(1, 2));
            }

            @Override
            protected void doClose() {
                assertTrue(stage.compareAndSet(2, 3));
            }
        };

        component.start();
        assertEquals(1, stage.get());
        if (randomBoolean()) {
            component.stop();
            assertEquals(2, stage.get());
        }
        component.close();
        assertEquals(3, stage.get());
    }

    public void testFailureAtStartup() {

        final var stage = new AtomicInteger();

        final var component = new AbstractLifecycleComponent() {
            @Override
            protected void doStart() {
                assertTrue(stage.compareAndSet(0, 1));
                throw new RuntimeException("failure");
            }

            @Override
            protected void doStop() {
                fail("unexpected");
            }

            @Override
            protected void doClose() {
                assertTrue(stage.compareAndSet(1, 3));
            }
        };

        expectThrows(RuntimeException.class, component::start);
        assertEquals(1, stage.get());
        component.close();
        assertEquals(3, stage.get());
    }
}
