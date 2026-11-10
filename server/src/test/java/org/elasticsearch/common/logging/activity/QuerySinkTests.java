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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;

public class QuerySinkTests extends ESTestCase {

    public void testAcceptReceivesTheContext() {
        List<TestContext> accepted = new ArrayList<>();
        var sink = new TestQuerySink<TestContext>(() -> true, accepted::add, () -> "test-sink");
        var context = new TestContext(42);

        sink.accept(context);

        assertThat(accepted, contains(context));
    }

    public void testIsEnabledIsReadEachTime() {
        var enabled = new AtomicBoolean(false);
        var sink = new TestQuerySink<TestContext>(enabled::get, context -> {}, () -> "test-sink");

        assertFalse(sink.isEnabled());
        enabled.set(true);
        assertTrue(sink.isEnabled());
    }

    public void testDescriptionIsEvaluatedLazily() {
        var evaluations = new AtomicInteger();
        var sink = new TestQuerySink<TestContext>(() -> true, context -> {}, () -> {
            evaluations.incrementAndGet();
            return "test-sink";
        });

        assertThat(evaluations.get(), equalTo(0));
        assertThat(sink.toString(), equalTo("test-sink"));
        assertThat(evaluations.get(), equalTo(1));
    }

    private static class TestContext extends QueryCaptureContext {
        TestContext(long tookInNanos) {
            super(mock(Task.class), "test", tookInNanos);
        }

        @Override
        public String getQuery() {
            return "";
        }

        @Override
        public int getResultCount() {
            return 0;
        }

        @Override
        public String[] getIndices() {
            return new String[0];
        }
    }
}
