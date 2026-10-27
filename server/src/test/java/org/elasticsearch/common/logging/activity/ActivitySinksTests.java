/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.logging.activity;

import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Optional;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.mock;

public class ActivitySinksTests extends ESTestCase {

    public void testEmptyHasNoSinks() {
        assertThat(ActivitySinks.EMPTY.forType(ContextA.class), empty());
    }

    public void testSinksAreRoutedByContextType() {
        var sinkA1 = sink(ContextA.class);
        var sinkA2 = sink(ContextA.class);
        var sinkB = sink(ContextB.class);
        var sinks = new ActivitySinks(
            List.of(provider(ContextA.class, sinkA1), provider(ContextB.class, sinkB), provider(ContextA.class, sinkA2))
        );

        assertThat(sinks.forType(ContextA.class), contains(sameInstance(sinkA1), sameInstance(sinkA2)));
        assertThat(sinks.forType(ContextB.class), contains(sameInstance(sinkB)));
    }

    public void testSinkForOtherContextTypeIsNotGiven() {
        var sinks = new ActivitySinks(List.of(provider(ContextA.class, sink(ContextA.class))));

        assertThat(sinks.forType(ContextB.class), empty());
    }

    private static <C extends ActivityCaptureContext> ActivitySink<C, C> sink(Class<C> type) {
        return new ActivitySink<>(() -> true, Optional::of, context -> {});
    }

    private static <C extends ActivityCaptureContext> ActivitySinkProvider<C> provider(Class<C> type, ActivitySink<C, ?> sink) {
        return new ActivitySinkProvider<>() {
            @Override
            public Class<C> contextType() {
                return type;
            }

            @Override
            public ActivitySink<C, ?> sink() {
                return sink;
            }
        };
    }

    private static class ContextA extends ActivityCaptureContext {
        ContextA() {
            super(mock(Task.class), "a", 0);
        }
    }

    private static class ContextB extends ActivityCaptureContext {
        ContextB() {
            super(mock(Task.class), "b", 0);
        }
    }
}
