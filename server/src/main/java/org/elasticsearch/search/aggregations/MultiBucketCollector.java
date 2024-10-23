/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations;

import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.MultiCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * A {@link BucketCollector} which allows running a bucket collection with several
 * {@link BucketCollector}s. It is similar to the {@link MultiCollector} except that the
 * {@link #wrap} method filters out the {@link BucketCollector#NO_OP_BUCKET_COLLECTOR}s and not
 * the null ones.
 */
public class MultiBucketCollector extends BucketCollector {
    /**
     * Wraps a list of {@link BucketCollector}s with a {@link MultiBucketCollector}. This
     * method works as follows:
     * <ul>
     * <li>Filters out the {@link BucketCollector#NO_OP_BUCKET_COLLECTOR}s collectors, so they are not used
     * during search time.
     * <li>If the input contains 1 real collector we wrap it in a collector that takes
     * {@code terminateIfNoop} into account.
     * <li>Otherwise the method returns a {@link MultiBucketCollector} which wraps the
     * non-{@link BucketCollector#NO_OP_BUCKET_COLLECTOR} collectors.
     * </ul>
     * @param terminateIfNoop Pass true if {@link #getLeafCollector} should throw
     * {@link CollectionTerminatedException} if all leaf collectors are noop. Pass
     * false if terminating would break stuff. The top level collection for
     * aggregations should pass true here because we want to skip collections if
     * all aggregations return NOOP. But when aggregtors themselves call this
     * method they chould *generally* pass false here because they have collection
     * actions to perform even if their sub-aggregators are NOOPs.
     */
    public static BucketCollector wrap(boolean terminateIfNoop, Iterable<? extends BucketCollector> collectors) {
        // For the user's convenience, we allow NO_OP collectors to be passed.
        // However, to improve performance, these null collectors are found
        // and dropped from the array we save for actual collection time.
        int n = 0;
        for (BucketCollector c : collectors) {
            if (c != NO_OP_BUCKET_COLLECTOR) {
                n++;
            }
        }

        if (n == 0) {
            return NO_OP_BUCKET_COLLECTOR;
        } else if (n == 1) {
            // only 1 Collector - return it.
            BucketCollector col = null;
            for (BucketCollector c : collectors) {
                if (c != null) {
                    col = c;
                    break;
                }
            }
            final BucketCollector collector = col;
            // Wrap the collector in one that takes terminateIfNoop into account.
            return new BucketCollector() {
                @Override
                public ScoreMode scoreMode() {
                    return collector.scoreMode();
                }

                @Override
                public void preCollection() throws IOException {
                    collector.preCollection();
                }

                @Override
                public void postCollection() throws IOException {
                    collector.postCollection();
                }

                @Override
                public LeafBucketCollector getLeafCollector(AggregationExecutionContext aggCtx) throws IOException {
                    try {
                        LeafBucketCollector leafCollector = collector.getLeafCollector(aggCtx);
                        if (false == leafCollector.isNoop()) {
                            return leafCollector;
                        }
                    } catch (CollectionTerminatedException e) {
                        throw new IllegalStateException(
                            "getLeafCollector should return a noop collector instead of throw "
                                + CollectionTerminatedException.class.getSimpleName(),
                            e
                        );
                    }
                    if (terminateIfNoop) {
                        throw new CollectionTerminatedException();
                    }
                    return LeafBucketCollector.NO_OP_COLLECTOR;
                }
            };
        } else {
            BucketCollector[] colls = new BucketCollector[n];
            n = 0;
            for (BucketCollector c : collectors) {
                if (c != null) {
                    colls[n++] = c;
                }
            }
            return new MultiBucketCollector(terminateIfNoop, colls);
        }
    }

    private final boolean terminateIfNoop;
    private final boolean cacheScores;
    private final BucketCollector[] collectors;

    private MultiBucketCollector(boolean terminateIfNoop, BucketCollector... collectors) {
        this.terminateIfNoop = terminateIfNoop;
        this.collectors = collectors;
        int numNeedsScores = 0;
        for (BucketCollector collector : collectors) {
            if (collector.scoreMode().needsScores()) {
                numNeedsScores += 1;
            }
        }
        this.cacheScores = numNeedsScores >= 2;
    }

    @Override
    public ScoreMode scoreMode() {
        ScoreMode scoreMode = null;
        for (BucketCollector collector : collectors) {
            if (scoreMode == null) {
                scoreMode = collector.scoreMode();
            } else if (scoreMode != collector.scoreMode()) {
                return ScoreMode.COMPLETE;
            }
        }
        return scoreMode;
    }

    @Override
    public void preCollection() throws IOException {
        for (BucketCollector collector : collectors) {
            collector.preCollection();
        }
    }

    @Override
    public void postCollection() throws IOException {
        for (BucketCollector collector : collectors) {
            collector.postCollection();
        }
    }

    @Override
    public String toString() {
        return Arrays.toString(collectors);
    }

    @Override
    public LeafBucketCollector getLeafCollector(AggregationExecutionContext aggCtx) throws IOException {
        final List<LeafBucketCollector> leafCollectors = new ArrayList<>(collectors.length);
        for (BucketCollector collector : collectors) {
            try {
                LeafBucketCollector leafCollector = collector.getLeafCollector(aggCtx);
                if (false == leafCollector.isNoop()) {
                    leafCollectors.add(leafCollector);
                }
            } catch (CollectionTerminatedException e) {
                throw new IllegalStateException(
                    "getLeafCollector should return a noop collector instead of throw "
                        + CollectionTerminatedException.class.getSimpleName(),
                    e
                );
            }
        }
        return switch (leafCollectors.size()) {
            case 0 -> {
                if (terminateIfNoop) {
                    throw new CollectionTerminatedException();
                }
                yield LeafBucketCollector.NO_OP_COLLECTOR;
            }
            case 1 -> leafCollectors.get(0);
            default -> new MultiLeafBucketCollector(leafCollectors, cacheScores);
        };
    }

    private static class MultiLeafBucketCollector extends LeafBucketCollector {

        private final boolean cacheScores;
        private final LeafBucketCollector[] collectors;
        private int numCollectors;
        private ScoreCachingScorable scorable;

        private MultiLeafBucketCollector(List<LeafBucketCollector> collectors, boolean cacheScores) {
            this.collectors = collectors.toArray(new LeafBucketCollector[collectors.size()]);
            this.cacheScores = cacheScores;
            this.numCollectors = this.collectors.length;
        }

        @Override
        public void setScorer(Scorable scorer) throws IOException {
            if (cacheScores) {
                scorable = new ScoreCachingScorable(scorer);
            }
            for (int i = 0; i < numCollectors; ++i) {
                final LeafCollector c = collectors[i];
                c.setScorer(cacheScores ? scorable : scorer);
            }
        }

        private void removeCollector(int i) {
            System.arraycopy(collectors, i + 1, collectors, i, numCollectors - i - 1);
            --numCollectors;
            collectors[numCollectors] = null;
        }

        @Override
        public void collect(int doc, long bucket) throws IOException {
            if (scorable != null) {
                scorable.curDoc = doc;
            }
            final LeafBucketCollector[] collectors = this.collectors;
            int numCollectors = this.numCollectors;
            for (int i = 0; i < numCollectors;) {
                final LeafBucketCollector collector = collectors[i];
                try {
                    collector.collect(doc, bucket);
                    ++i;
                } catch (CollectionTerminatedException e) {
                    removeCollector(i);
                    numCollectors = this.numCollectors;
                    if (numCollectors == 0) {
                        throw new CollectionTerminatedException();
                    }
                }
            }
        }
    }

    private static class ScoreCachingScorable extends Scorable {

        private final Scorable in;
        private int curDoc = -1; // current document
        private int scoreDoc = -1; // document that score was computed on
        private float score;

        ScoreCachingScorable(Scorable in) {
            this.in = in;
        }

        @Override
        public float score() throws IOException {
            if (curDoc != scoreDoc) {
                score = in.score();
                scoreDoc = curDoc;
            }
            return score;
        }
    }
}
