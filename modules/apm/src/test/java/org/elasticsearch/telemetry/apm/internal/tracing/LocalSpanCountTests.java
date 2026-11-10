/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal.tracing;

import io.opentelemetry.context.Context;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class LocalSpanCountTests extends ESTestCase {

    public void testContextWithoutCountHasNone() {
        assertThat(LocalSpanCount.getOrNull(Context.root()), nullValue());
    }

    public void testStoredCountIsReadFromTheContext() {
        final LocalSpanCount count = new LocalSpanCount();

        final Context context = Context.root().with(count);

        assertThat(LocalSpanCount.getOrNull(context), sameInstance(count));
    }

    public void testIncrementAndGetCounts() {
        final LocalSpanCount count = new LocalSpanCount();

        assertThat(count.incrementAndGet(), equalTo(1));
        assertThat(count.incrementAndGet(), equalTo(2));
        assertThat(count.incrementAndGet(), equalTo(3));
    }

    public void testDecrementGivesTheCountBack() {
        final LocalSpanCount count = new LocalSpanCount();
        count.incrementAndGet();
        count.incrementAndGet();

        count.decrement();

        assertThat(count.incrementAndGet(), equalTo(2));
    }
}
