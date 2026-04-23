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

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class ActivityLoggerContextBuilderTests extends ESTestCase {

    public void testElapsed() {
        Task task = mock(Task.class);
        AtomicLong currentTime = new AtomicLong(1000);

        TestContextBuilder builder = new TestContextBuilder(task, "test_request", currentTime::get);

        currentTime.set(1500);
        assertThat(builder.elapsed(), is(500L));

        currentTime.set(2000);
        assertThat(builder.elapsed(), is(1000L));
    }

    public void testBuildSuccess() {
        Task task = mock(Task.class);
        AtomicLong currentTime = new AtomicLong(1000);

        TestContextBuilder builder = new TestContextBuilder(task, "test_request", currentTime::get);
        currentTime.set(1500);

        TestContext context = builder.build("test_response");

        assertThat(context.getTookInNanos(), is(500L));
        assertThat(context.isSuccess(), is(true));
        assertThat(context.getType(), is("test"));
    }

    public void testBuildFailure() {
        Task task = mock(Task.class);
        AtomicLong currentTime = new AtomicLong(1000);

        TestContextBuilder builder = new TestContextBuilder(task, "test_request", currentTime::get);
        currentTime.set(1500);

        TestContext context = builder.build(new RuntimeException("test error"));

        assertThat(context.getTookInNanos(), is(500L));
        assertThat(context.isSuccess(), is(false));
        assertThat(context.getErrorMessage(), is("test error"));
        assertThat(context.getErrorType(), is(RuntimeException.class.getName()));
        assertThat(context.getType(), is("test"));
    }

    private static class TestContext extends ActivityLoggerContext {
        TestContext(Task task, String type, long tookInNanos, Exception error) {
            super(task, type, tookInNanos, error);
        }
    }

    private static class TestContextBuilder extends ActivityLoggerContextBuilder<TestContext, String, String> {
        protected TestContextBuilder(Task task, String request, LongSupplier nanoTimeSupplier) {
            super(task, request, nanoTimeSupplier);
        }

        @Override
        public TestContext build(String response) {
            return new TestContext(task, "test", elapsed(), null);
        }

        @Override
        public TestContext build(Exception e) {
            return new TestContext(task, "test", elapsed(), e);
        }
    }
}
