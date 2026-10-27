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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.mockito.Mockito.mock;

public class ActivitySinkTests extends ESTestCase {

    public void testWriterReceivesWhatTheProducerMakesOfTheContext() {
        List<String> written = new ArrayList<>();
        var sink = new ActivitySink<TestContext, String>(
            () -> true,
            context -> Optional.of("took " + context.getTookInNanos()),
            written::add
        );

        sink.accept(new TestContext(42));

        assertThat(written, contains("took 42"));
    }

    public void testEmptyFromProducerWritesNothing() {
        List<String> written = new ArrayList<>();
        var sink = new ActivitySink<TestContext, String>(() -> true, context -> Optional.empty(), written::add);

        sink.accept(new TestContext(42));

        assertThat(written, empty());
    }

    public void testIsEnabledIsReadEachTime() {
        var enabled = new AtomicBoolean(false);
        var sink = new ActivitySink<TestContext, String>(enabled::get, context -> Optional.empty(), message -> {});

        assertFalse(sink.isEnabled());
        enabled.set(true);
        assertTrue(sink.isEnabled());
    }

    private static class TestContext extends ActivityCaptureContext {
        TestContext(long tookInNanos) {
            super(mock(Task.class), "test", tookInNanos);
        }
    }
}
