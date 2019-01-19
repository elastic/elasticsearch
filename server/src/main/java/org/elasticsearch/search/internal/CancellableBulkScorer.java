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

package org.elasticsearch.search.internal;

import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.util.Bits;

import java.io.IOException;
import java.util.Objects;

/**
 * A {@link BulkScorer} wrapper that runs a {@link Runnable} on a regular basis
 * so that the query can be interrupted.
 */
final class CancellableBulkScorer extends BulkScorer {

    // we use the BooleanScorer window size as a base interval in order to make sure that we do not
    // slow down boolean queries
    private static final int INITIAL_INTERVAL = 1 << 11;

    // No point in having intervals that are larger than 1M
    private static final int MAX_INTERVAL = 1 << 20;

    private final BulkScorer scorer;
    private final Runnable checkCancelled;

    CancellableBulkScorer(BulkScorer scorer, Runnable checkCancelled) {
        this.scorer = Objects.requireNonNull(scorer);
        this.checkCancelled = Objects.requireNonNull(checkCancelled);
    }

    @Override
    public int score(LeafCollector collector, Bits acceptDocs, int min, int max) throws IOException {
        int interval = INITIAL_INTERVAL;
        while (min < max) {
            checkCancelled.run();
            final int newMax = (int) Math.min((long) min + interval, max);
            min = scorer.score(collector, acceptDocs, min, newMax);
            interval = Math.min(interval << 1, MAX_INTERVAL);
        }
        checkCancelled.run();
        return min;
    }

    @Override
    public long cost() {
        return scorer.cost();
    }

}
