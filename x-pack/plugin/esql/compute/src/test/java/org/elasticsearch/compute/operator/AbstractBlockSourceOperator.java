/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.compute.data.Page;
import org.elasticsearch.test.ESTestCase;

/**
 * An abstract source operator. Implementations of this operator produce pages with a random
 * number of positions up to a maximum of the given maxPagePositions positions.
 */
public abstract class AbstractBlockSourceOperator extends SourceOperator {

    private final int maxPagePositions;

    private boolean finished;

    /** The position of the next element to output. */
    protected int currentPosition;

    protected AbstractBlockSourceOperator(int maxPagePositions) {
        this.maxPagePositions = maxPagePositions;
    }

    /** The number of remaining elements that this source operator will produce. */
    protected abstract int remaining();

    /** Creates a page containing a block with {@code length} positions, from the given position offset. */
    protected abstract Page createPage(int positionOffset, int length);

    @Override
    public final Page getOutput() {
        if (finished) {
            return null;
        }
        if (remaining() <= 0) {
            finish();
            return null;
        }
        int length = Math.min(ESTestCase.randomInt(maxPagePositions), remaining());
        return createPage(currentPosition, length);
    }

    @Override
    public final void close() {}

    @Override
    public final boolean isFinished() {
        return finished;
    }

    @Override
    public final void finish() {
        finished = true;
    }
}
