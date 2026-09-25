/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal;

import io.opentelemetry.context.Context;
import io.opentelemetry.context.ContextKey;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;

import java.util.function.UnaryOperator;

public class OtelContextTests extends ESTestCase {

    private static final ContextKey<String> KEY = ContextKey.named("test-key");

    public void test_updateAndGet_whenAbsent_receivesRootContext() {
        var ctx = threadContext();
        assertSame(Context.root(), OtelContext.updateAndGet(ctx, UnaryOperator.identity()));
    }

    public void test_updateAndGet_whenPresent_receivesPreviousContext() {
        var ctx = threadContext();
        var existing = Context.root().with(KEY, "existing");
        ctx.putTransient(Task.APM_TRACE_CONTEXT, existing);

        assertSame(existing, OtelContext.updateAndGet(ctx, UnaryOperator.identity()));
    }

    public void test_updateAndGet_writesResultToThreadContext() {
        var ctx = threadContext();
        var updated = Context.root().with(KEY, "new-value");
        var result = OtelContext.updateAndGet(ctx, ignored -> updated);
        assertSame(updated, result);
        assertSame(updated, ctx.getTransient(Task.APM_TRACE_CONTEXT));
    }

    public void test_updateAndGet_preservesExistingKeysWhenLayering() {
        var ctx = threadContext();
        var otherKey = ContextKey.<String>named("other");
        ctx.putTransient(Task.APM_TRACE_CONTEXT, Context.root().with(otherKey, "other-value"));

        OtelContext.updateAndGet(ctx, prev -> prev.with(KEY, "layered"));

        Context result = ctx.getTransient(Task.APM_TRACE_CONTEXT);
        assertEquals("layered", result.get(KEY));
        assertEquals("other-value", result.get(otherKey));
    }

    public void test_getValueOrNullFromContext_whenBothAbsent_returnsNull() {
        assertNull(OtelContext.getValueOrNullFromContext(threadContext(), KEY));
    }

    public void test_getValueOrNullFromContext_whenCurrentHasKey_returnsValue() {
        var ctx = threadContext();
        ctx.putTransient(Task.APM_TRACE_CONTEXT, Context.root().with(KEY, "current"));
        assertEquals("current", OtelContext.getValueOrNullFromContext(ctx, KEY));
    }

    public void test_getValueOrNullFromContext_whenOnlyParentHasKey_returnsParent() {
        var ctx = threadContext();
        ctx.putTransient(Task.PARENT_APM_TRACE_CONTEXT, Context.root().with(KEY, "parent"));
        assertEquals("parent", OtelContext.getValueOrNullFromContext(ctx, KEY));
    }

    public void test_getValueOrNullFromContext_whenBothHaveKey_currentWins() {
        var ctx = threadContext();
        ctx.putTransient(Task.APM_TRACE_CONTEXT, Context.root().with(KEY, "current"));
        ctx.putTransient(Task.PARENT_APM_TRACE_CONTEXT, Context.root().with(KEY, "parent"));
        assertEquals("current", OtelContext.getValueOrNullFromContext(ctx, KEY));
    }

    public void test_getValueOrNullFromContext_whenCurrentLacksKey_fallsBackToParent() {
        var ctx = threadContext();
        ctx.putTransient(Task.APM_TRACE_CONTEXT, Context.root());
        ctx.putTransient(Task.PARENT_APM_TRACE_CONTEXT, Context.root().with(KEY, "parent"));
        assertEquals("parent", OtelContext.getValueOrNullFromContext(ctx, KEY));
    }

    private ThreadContext threadContext() {
        return new ThreadContext(Settings.EMPTY);
    }
}
