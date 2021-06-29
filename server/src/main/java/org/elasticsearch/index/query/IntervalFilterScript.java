/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.query;

import org.apache.lucene.queries.intervals.IntervalIterator;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptFactory;

/**
 * Base class for scripts used as interval filters, see {@link IntervalsSourceProvider.IntervalFilter}
 */
public abstract class IntervalFilterScript {

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

    public static final String[] PARAMETERS = new String[]{ "interval" };
    public static final ScriptContext<Factory> CONTEXT = new ScriptContext<>("interval", Factory.class);

}
