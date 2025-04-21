/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.transport;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.SingleResultDeduplicator;
import org.elasticsearch.action.StrictSingleResultDeduplicator;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;

import java.util.function.Consumer;

public class StrictSingleResultDeduplicatorTests extends SingleResultDeduplicatorTests {

    @Override
    protected <T> SingleResultDeduplicator<T> makeSingleResultDeduplicator(
        ThreadContext threadContext,
        Consumer<ActionListener<T>> executeAction
    ) {
        return new StrictSingleResultDeduplicator<T>(threadContext, executeAction);
    }

    public void testDeduplicatesWithoutShowingStaleData() {
        final SetOnce<ActionListener<Object>> firstListenerRef = new SetOnce<>();
        final SetOnce<ActionListener<Object>> secondListenerRef = new SetOnce<>();
        final Object result1 = new Object();
        final Object result2 = new Object();
        final var deduplicator = makeSingleResultDeduplicator(new ThreadContext(Settings.EMPTY), l -> {
            if (firstListenerRef.trySet(l) == false) {
                secondListenerRef.set(l);
            }
        });

        final int totalListeners = randomIntBetween(2, 10);
        final boolean[] called = new boolean[totalListeners];
        deduplicator.execute(ActionTestUtils.assertNoFailureListener(response -> {
            assertFalse(called[0]);
            called[0] = true;
            assertEquals(result1, response);
        }));

        for (int i = 1; i < totalListeners; i++) {
            final int index = i;
            deduplicator.execute(ActionTestUtils.assertNoFailureListener(response -> {
                assertFalse(called[index]);
                called[index] = true;
                assertEquals(result2, response);
            }));
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
