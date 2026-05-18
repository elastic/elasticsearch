/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.query;

import org.apache.lucene.queries.intervals.IntervalIterator;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptFactory;

/**
 * Base class for scripts used as interval filters, see {@link IntervalsSourceProvider.IntervalFilter}
 */
public abstract class IntervalFilterScript {

    private Runnable cancellationCheck = null;

    /**
     * Stores a runnable invoked between loop iterations by the painless engine to check for
     * search timeout or task cancellation. The runnable throws when execution should abort;
     * {@code null} disables the check.
     */
    public void _setCancellationCheck(Runnable cancellationCheck) {
        this.cancellationCheck = cancellationCheck;
    }

    /** Returns the runnable set by {@link #_setCancellationCheck}, or {@code null}. */
    public Runnable _getCancellationCheck() {
        return cancellationCheck;
    }

    public static class Interval {

        private IntervalIterator iterator;

        void setIterator(IntervalIterator iterator) {
            this.iterator = iterator;
        }

        public int getStart() {
            return iterator.start();
        }

        public int getEnd() {
            return iterator.end();
        }

        public int getGaps() {
            return iterator.gaps();
        }
    }

    public abstract boolean execute(Interval interval);

    public interface Factory extends ScriptFactory {
        IntervalFilterScript newInstance();
    }

    public static final String[] PARAMETERS = new String[] { "interval" };
    public static final ScriptContext<Factory> CONTEXT = new ScriptContext<>("interval", Factory.class);

}
