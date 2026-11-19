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
 * A column's prefetch as ordered steps from documents to their bytes. A stage runs steps until one prefetches something,
 * so a step with nothing to prefetch costs no stage.
 */
public final class PrefetchSteps implements PrefetchableDocValues {

    /** One step of a column's prefetch, which reads only what earlier steps prefetched. */
    @FunctionalInterface
    public interface Step {
        /**
         * Prefetches what it can locate for {@code docs[from, to)}.
         *
         * @return whether it prefetched anything
         */
        boolean run(int[] docs, int from, int to) throws IOException;
    }

    /** {@code steps} run as one step. */
    public static Step all(Step... steps) {
        return (docs, from, to) -> {
            boolean prefetched = false;
            for (Step step : steps) {
                prefetched |= step.run(docs, from, to);
            }
            return prefetched;
        };
    }

    private final Step[] steps;
    private int next;

    public PrefetchSteps(Step... steps) {
        this.steps = steps;
    }

    @Override
    public boolean prefetch(int stage, int[] docs, int from, int to) throws IOException {
        if (stage == 0) {
            next = 0;
        }
        while (next < steps.length) {
            boolean prefetched = steps[next++].run(docs, from, to);
            if (prefetched && next < steps.length) {
                return true;
            }
        }
        return false;
    }
}
