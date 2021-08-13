/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.query;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.FilterCollector;
import org.apache.lucene.search.FilterLeafCollector;
import org.apache.lucene.search.LeafCollector;

import java.io.IOException;

/**
 * A {@link Collector} that early terminates collection after <code>maxCountHits</code> docs have been collected.
 */
public class EarlyTerminatingCollector extends FilterCollector {
    static final class EarlyTerminationException extends RuntimeException {
        EarlyTerminationException(String msg) {
            super(msg);
        }
    }

    private final int maxCountHits;
    private int numCollected;
    private boolean forceTermination;
    private boolean earlyTerminated;

    /**
     * Ctr
     * @param delegate The delegated collector.
     * @param maxCountHits The number of documents to collect before termination.
     * @param forceTermination Whether the collection should be terminated with an exception ({@link EarlyTerminationException})
     *                         that is not caught by other {@link Collector} or with a {@link CollectionTerminatedException} otherwise.
     */
    EarlyTerminatingCollector(final Collector delegate, int maxCountHits, boolean forceTermination) {
        super(delegate);
        this.maxCountHits = maxCountHits;
        this.forceTermination = forceTermination;
    }

    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
        if (numCollected >= maxCountHits) {
            earlyTerminated = true;
            if (forceTermination) {
                throw new EarlyTerminationException("early termination [CountBased]");
            } else {
                throw new CollectionTerminatedException();
            }
        }
        return new FilterLeafCollector(super.getLeafCollector(context)) {
            @Override
            public void collect(int doc) throws IOException {
                if (++numCollected > maxCountHits) {
                    earlyTerminated = true;
                    if (forceTermination) {
                        throw new EarlyTerminationException("early termination [CountBased]");
                    } else {
                        throw new CollectionTerminatedException();
                    }
                }
                super.collect(doc);
            }
        };
    }

    /**
     * Returns true if this collector has early terminated.
     */
    public boolean hasEarlyTerminated() {
        return earlyTerminated;
    }
}
