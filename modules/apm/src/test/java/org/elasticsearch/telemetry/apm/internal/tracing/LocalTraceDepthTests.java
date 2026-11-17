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

public class LocalTraceDepthTests extends ESTestCase {

    public void testContextWithoutDepthIsAtTheRoot() {
        assertThat(LocalTraceDepth.get(Context.root()), equalTo(new LocalTraceDepth(0)));
    }

    public void testIncrementReturnsTheDepthOfTheChild() {
        final LocalTraceDepth depth = new LocalTraceDepth(2);

        assertThat(depth.increment(), equalTo(new LocalTraceDepth(3)));
        // immutable
        assertThat(depth.depth(), equalTo(2));
    }

    public void testStoredDepthIsReadFromTheContext() {
        final int depth = randomIntBetween(0, 100);

        final Context context = Context.root().with(new LocalTraceDepth(depth));

        assertThat(LocalTraceDepth.get(context).depth(), equalTo(depth));
    }

    public void testStoringReplacesTheDepthOfTheContext() {
        final Context parent = Context.root().with(new LocalTraceDepth(1));

        final Context child = parent.with(LocalTraceDepth.get(parent).increment());

        assertThat(LocalTraceDepth.get(child).depth(), equalTo(2));
        assertThat(LocalTraceDepth.get(parent).depth(), equalTo(1));
    }
}
