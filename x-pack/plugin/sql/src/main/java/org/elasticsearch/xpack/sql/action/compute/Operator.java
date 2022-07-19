/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.action.compute;

import org.elasticsearch.action.support.ListenableActionFuture;
import org.elasticsearch.core.TimeValue;

public interface Operator {

    // whether the given operator can accept more input pages
    boolean needsInput();

    // adds input page to operator. only called when needsInput() == true
    void addInput(Page page);

    // tells the operator that it won't receive more input pages
    void finish();

    // whether the operator has finished processing all input pages and made the corresponding output page available
    boolean isFinished();

    // returns non-null if output available
    Page getOutput();

    // tells the operator that it won't be used anymore, and it's resources can be cleaned up
    void close();

    // returns a future that completes when the operator becomes unblocked
    default ListenableActionFuture<Void> isBlocked() {
        return NOT_BLOCKED;
    }

    ListenableActionFuture<Void> NOT_BLOCKED = newCompletedFuture();

    static ListenableActionFuture<Void> newCompletedFuture() {
        ListenableActionFuture<Void> fut = new ListenableActionFuture<>();
        fut.onResponse(null);
        return fut;
    }
}
