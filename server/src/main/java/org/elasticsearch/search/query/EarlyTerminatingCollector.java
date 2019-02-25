/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
