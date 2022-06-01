/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.test.ESTestCase;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.equalTo;

public class AllocationActionListenerTest extends ESTestCase {

    public void testShouldDelegateWhenBothComplete() {
        var completed = new AtomicBoolean(false);
        var listener = new AllocationActionListener<AcknowledgedResponse>(
            ActionListener.wrap(ignore -> completed.set(true), exception -> { throw new AssertionError("Should not fail in test"); })
        );

        listener.clusterStateUpdate().onResponse(AcknowledgedResponse.TRUE);
        listener.rereoute().onResponse(null);

        assertThat(completed.get(), equalTo(true));
    }

    public void testShouldNotDelegateWhenOnlyOneComplete() {
        var completed = new AtomicBoolean(false);
        var listener = new AllocationActionListener<AcknowledgedResponse>(
            ActionListener.wrap(ignore -> completed.set(true), exception -> { throw new AssertionError("Should not fail in test"); })
        );

        if (randomBoolean()) {
            listener.clusterStateUpdate().onResponse(AcknowledgedResponse.TRUE);
        } else {
            listener.rereoute().onResponse(null);
        }

        assertThat(completed.get(), equalTo(false));
    }

    public void testShouldDelegateFailureImmediately() {
        var completed = new AtomicBoolean(false);
        var listener = new AllocationActionListener<AcknowledgedResponse>(
            ActionListener.wrap(ignore -> { throw new AssertionError("Should not complete in test"); }, exception -> completed.set(true))
        );

        if (randomBoolean()) {
            listener.clusterStateUpdate().onFailure(new RuntimeException());
        } else {
            listener.rereoute().onFailure(new RuntimeException());
        }

        assertThat(completed.get(), equalTo(true));
    }
}
