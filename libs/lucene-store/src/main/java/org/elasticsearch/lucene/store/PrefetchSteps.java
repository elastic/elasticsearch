/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene.store;

import java.io.IOException;

/**
 * A column's prefetch as ordered steps from a document to its bytes, each reading only what earlier steps asked for. A
 * stage runs steps until one asks for something, so a step with nothing to ask for costs no stage. What a stage asks for
 * is sent when it ends, through its {@link PrefetchRanges}.
 */
public final class PrefetchSteps implements PrefetchableDocValues {

    /** One step of a column's prefetch. */
    @FunctionalInterface
    public interface Step {
        /**
         * Reads what earlier steps asked for and asks for what it can now locate for {@code docs[from, to)}.
         *
         * @return whether it asked for anything
         */
        boolean run(int[] docs, int from, int to) throws IOException;
    }

    /** {@code steps} run as one step. */
    public static Step all(Step... steps) {
        return (docs, from, to) -> {
            boolean asked = false;
            for (Step step : steps) {
                asked |= step.run(docs, from, to);
            }
            return asked;
        };
    }

    private final PrefetchRanges ranges;
    private final Step[] steps;
    private int next;

    public PrefetchSteps(PrefetchRanges ranges, Step... steps) {
        this.ranges = ranges;
        this.steps = steps;
    }

    @Override
    public boolean prefetch(int stage, int[] docs, int from, int to, int readUnit) throws IOException {
        if (stage == 0) {
            next = 0;
        }
        ranges.readUnit(readUnit);
        try {
            while (next < steps.length) {
                boolean asked = steps[next++].run(docs, from, to);
                if (asked && next < steps.length) {
                    return true;
                }
            }
            return false;
        } finally {
            ranges.flush();
        }
    }
}
