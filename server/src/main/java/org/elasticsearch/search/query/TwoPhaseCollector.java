/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.query;

import org.apache.lucene.search.Collector;
import org.apache.lucene.search.FilterCollector;
import org.apache.lucene.search.MultiCollector;

import java.io.IOException;

/** A {@link Collector} extension that allows to run a post collection phase. This phase
 * is run on the same thread as the collection phase. */
public interface TwoPhaseCollector extends Collector {

    /** run post collection phase */
    void postCollection() throws IOException;

    /** Get the wrapped collector if it exists. This si done to make the profile API happy. */
    Collector getCollector();

    /** Wraps an array of {@link TwoPhaseCollector}, the post-collection phase is called for each lement of the array */
    static TwoPhaseCollector wrapCollection(TwoPhaseCollector... twoPhaseCollectors) {
        Collector multi = MultiCollector.wrap(twoPhaseCollectors);
        return new TwoPhaseCollectorImpl(multi) {
            @Override
            public void postCollection() throws IOException {
                for (TwoPhaseCollector twoPhaseCollector : twoPhaseCollectors) {
                    twoPhaseCollector.postCollection();
                }
            }
        };
    }

    /** Wraps a {@link Collector}, the post-collection phase is an NO-OP */
    static TwoPhaseCollector wrap(Collector collector) {
        return new TwoPhaseCollectorImpl(collector) {
            @Override
            public void postCollection() {}
        };
    }

    /** Wraps a {@link Collector} that is wrapping itself a {@link TwoPhaseCollector}, the post-collection phase
     * is called on the wrapped {@link TwoPhaseCollector} */
    static TwoPhaseCollector wrap(Collector wrappedCollector, TwoPhaseCollector innerCollector) {
        return new TwoPhaseCollectorImpl(wrappedCollector) {
            @Override
            public void postCollection() throws IOException {
                innerCollector.postCollection();
            }
        };
    }

    abstract class TwoPhaseCollectorImpl extends FilterCollector implements TwoPhaseCollector {

        private TwoPhaseCollectorImpl(Collector collector) {
            super(collector);
        }

        @Override
        public Collector getCollector() {
            return in;
        }
    }
}
