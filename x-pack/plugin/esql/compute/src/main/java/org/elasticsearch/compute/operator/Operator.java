/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.common.io.stream.VersionedNamedWriteable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.compute.Describable;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.xcontent.ToXContentObject;

/**
 * Operator is low-level building block that consumes, transforms and produces data.
 * An operator can have state, and assumes single-threaded access.
 * Data is processed in smaller batches (termed {@link Page}s) that are passed to
 * (see {@link #addInput(Page)}) or retrieved from (see {@link #getOutput()} operators.
 * The component that's in charge of passing data between operators is the {@link Driver}.
 *
 * More details on how this integrates with other components can be found in the package documentation of
 * {@link org.elasticsearch.compute}
 */
public interface Operator extends Releasable {
    /**
     * Target number of bytes in a page. By default we'll try and size pages
     * so that they contain this many bytes.
     */
    int TARGET_PAGE_SIZE = Math.toIntExact(ByteSizeValue.ofKb(256).getBytes());

    /**
     * The minimum number of positions for a {@link SourceOperator} to
     * target generating. This isn't 1 because {@link Block}s have
     * non-trivial overhead and it's just not worth building even
     * smaller blocks without under normal circumstances.
     */
    int MIN_TARGET_PAGE_SIZE = 32;

    /**
     * whether the given operator can accept more input pages
     */
    boolean needsInput();

    /**
     * adds an input page to the operator. only called when needsInput() == true and isFinished() == false
     * @throws UnsupportedOperationException  if the operator is a {@link SourceOperator}
     */
    void addInput(Page page);

    /**
     * notifies the operator that it won't receive any more input pages
     */
    void finish();

    /**
     * whether the operator has finished processing all input pages and made the corresponding output pages available
     */
    boolean isFinished();

    /**
     * returns non-null if output page available. Only called when isFinished() == false
     * @throws UnsupportedOperationException  if the operator is a {@link SinkOperator}
     */
    Page getOutput();

    /**
     * notifies the operator that it won't be used anymore (i.e. none of the other methods called),
     * and its resources can be cleaned up
     */
    @Override
    void close();

    /**
     * The status of the operator.
     */
    default Status status() {
        return null;
    }

    /**
     * An operator can be blocked on some action (e.g. waiting for some resources to become available).
     * If so, it returns a future that completes when the operator becomes unblocked.
     * If the operator is not blocked, this method returns {@link #NOT_BLOCKED} which is an already
     * completed future.
     */
    default IsBlockedResult isBlocked() {
        return NOT_BLOCKED;
    }

    IsBlockedResult NOT_BLOCKED = new IsBlockedResult(SubscribableListener.newSucceeded(null), "not blocked");

    /**
     * A factory for creating intermediate operators.
     */
    interface OperatorFactory extends Describable {
        /** Creates a new intermediate operator. */
        Operator get(DriverContext driverContext);
    }

    /**
     * Status of an {@link Operator} to be returned by the tasks API.
     */
    interface Status extends ToXContentObject, VersionedNamedWriteable {}
}
