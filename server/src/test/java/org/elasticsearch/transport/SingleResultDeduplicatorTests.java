/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.SingleResultDeduplicator;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESTestCase;

public class SingleResultDeduplicatorTests extends ESTestCase {

    public void testDeduplicatesWithoutShowingStaleData() {
        final SetOnce<ActionListener<Object>> firstListenerRef = new SetOnce<>();
        final SetOnce<ActionListener<Object>> secondListenerRef = new SetOnce<>();
        final SingleResultDeduplicator<Object> deduplicator = new SingleResultDeduplicator<>(new ThreadContext(Settings.EMPTY), l -> {
            if (firstListenerRef.trySet(l) == false) {
                secondListenerRef.set(l);
            }
        });
        final Object result1 = new Object();
        final Object result2 = new Object();

        final int totalListeners = randomIntBetween(2, 10);
        final boolean[] called = new boolean[totalListeners];
        deduplicator.execute(new ActionListener<Object>() {
            @Override
            public void onResponse(Object response) {
                assertFalse(called[0]);
                called[0] = true;
                assertEquals(result1, response);
            }

            @Override
            public void onFailure(Exception e) {
                throw new AssertionError(e);
            }
        });

        for (int i = 1; i < totalListeners; i++) {
            final int index = i;
            deduplicator.execute(new ActionListener<Object>() {

                @Override
                public void onResponse(Object response) {
                    assertFalse(called[index]);
                    called[index] = true;
                    assertEquals(result2, response);
                }

                @Override
                public void onFailure(Exception e) {
                    throw new AssertionError(e);
                }
            });
        }
        for (int i = 0; i < totalListeners; i++) {
            assertFalse(called[i]);
        }
        firstListenerRef.get().onResponse(result1);
        assertTrue(called[0]);
        for (int i = 1; i < totalListeners; i++) {
            assertFalse(called[i]);
        }
        secondListenerRef.get().onResponse(result2);
        for (int i = 0; i < totalListeners; i++) {
            assertTrue(called[i]);
        }
    }
}
