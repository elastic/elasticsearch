package org.elasticsearch.index.query;

import org.apache.lucene.search.intervals.IntervalIterator;
import org.elasticsearch.script.ScriptContext;

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

    public interface Factory {
        IntervalFilterScript newInstance();
    }

    public static final String[] PARAMETERS = new String[]{ "interval" };
    public static final ScriptContext<Factory> CONTEXT = new ScriptContext<>("interval", Factory.class);

}
