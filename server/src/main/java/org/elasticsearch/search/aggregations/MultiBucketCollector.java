/* @notice
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Modifications copyright (C) 2020 Elasticsearch B.V.
 */

package org.elasticsearch.search.aggregations;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.FilterScorable;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.MultiCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreCachingWrappingScorer;
import org.apache.lucene.search.ScoreMode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * A {@link BucketCollector} which allows running a bucket collection with several
 * {@link BucketCollector}s. It is similar to the {@link MultiCollector} except that the
 * {@link #wrap} method filters out the {@link BucketCollector#NO_OP_COLLECTOR}s and not
 * the null ones.
 */
public class MultiBucketCollector extends BucketCollector {
    /**
     * Wraps a list of {@link BucketCollector}s with a {@link MultiBucketCollector}. This
     * method works as follows:
     * <ul>
     * <li>Filters out the {@link BucketCollector#NO_OP_COLLECTOR}s collectors, so they are not used
     * during search time.
     * <li>If the input contains 1 real collector we wrap it in a collector that takes
     * {@code terminateIfNoop} into account.
     * <li>Otherwise the method returns a {@link MultiBucketCollector} which wraps the
     * non-{@link BucketCollector#NO_OP_COLLECTOR} collectors.
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
            if (c != NO_OP_COLLECTOR) {
                n++;
            }
        }

        if (n == 0) {
            return NO_OP_COLLECTOR;
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
        for (Collector collector : collectors) {
            if (collector.scoreMode().needsScores()) {
                numNeedsScores += 1;
            }
        }
        this.cacheScores = numNeedsScores >= 2;
    }

    @Override
    public ScoreMode scoreMode() {
        ScoreMode scoreMode = null;
        for (Collector collector : collectors) {
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
    public LeafBucketCollector getLeafCollector(LeafReaderContext context) throws IOException {
        final List<LeafBucketCollector> leafCollectors = new ArrayList<>(collectors.length);
        for (BucketCollector collector : collectors) {
            try {
                LeafBucketCollector leafCollector = collector.getLeafCollector(context);
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
        switch (leafCollectors.size()) {
            case 0:
                if (terminateIfNoop) {
                    throw new CollectionTerminatedException();
                }
                return LeafBucketCollector.NO_OP_COLLECTOR;

            case 1:
                // we also wrap single collector in case setMinCompetitiveScore is called and
                // the global score mode is not ScoreMode.TOP_SCORES
            default:
                return new MultiLeafBucketCollector(leafCollectors, cacheScores, scoreMode() == ScoreMode.TOP_SCORES);
        }
    }

    private static class MultiLeafBucketCollector extends LeafBucketCollector {
        private final boolean cacheScores;
        private final LeafBucketCollector[] collectors;
        private final float[] minScores;
        private final boolean skipNonCompetitiveScores;

        private MultiLeafBucketCollector(List<LeafBucketCollector> collectors, boolean cacheScores, boolean skipNonCompetitive) {
            this.collectors = collectors.toArray(new LeafBucketCollector[collectors.size()]);
            this.cacheScores = cacheScores;
            this.skipNonCompetitiveScores = skipNonCompetitive;
            this.minScores = this.skipNonCompetitiveScores ? new float[this.collectors.length] : null;
        }

        @Override
        public void setScorer(Scorable scorer) throws IOException {
            if (cacheScores) {
                scorer = new ScoreCachingWrappingScorer(scorer);
            }
            if (skipNonCompetitiveScores) {
                for (int i = 0; i < collectors.length; ++i) {
                    final LeafCollector c = collectors[i];
                    if (c != null) {
                        c.setScorer(new MinCompetitiveScoreAwareScorable(scorer,  i,  minScores));
                    }
                }
            } else {
                scorer = new FilterScorable(scorer) {
                    @Override
                    public void setMinCompetitiveScore(float minScore) throws IOException {
                        // Ignore calls to setMinCompetitiveScore so that if we wrap two
                        // collectors and one of them wants to skip low-scoring hits, then
                        // the other collector still sees all hits.
                    }

                };
                for (int i = 0; i < collectors.length; ++i) {
                    final LeafCollector c = collectors[i];
                    if (c != null) {
                        c.setScorer(scorer);
                    }
                }
            }
        }

        @Override
        public void collect(int doc, long bucket) throws IOException {
            for (int i = 0; i < collectors.length; i++) {
                final LeafBucketCollector collector = collectors[i];
                if (collector != null) {
                    try {
                        collector.collect(doc, bucket);
                    } catch (CollectionTerminatedException e) {
                        collectors[i] = null;
                        if (allCollectorsTerminated()) {
                            throw new CollectionTerminatedException();
                        }
                    }
                }
            }
        }

        private boolean allCollectorsTerminated() {
            for (int i = 0; i < collectors.length; i++) {
                if (collectors[i] != null) {
                    return false;
                }
            }
            return true;
        }
    }

    static final class MinCompetitiveScoreAwareScorable extends FilterScorable {
        private final int idx;
        private final float[] minScores;

        MinCompetitiveScoreAwareScorable(Scorable in, int idx, float[] minScores) {
            super(in);
            this.idx = idx;
            this.minScores = minScores;
        }

        @Override
        public void setMinCompetitiveScore(float minScore) throws IOException {
            if (minScore > minScores[idx]) {
                minScores[idx] = minScore;
                in.setMinCompetitiveScore(minScore());
            }
        }

        private float minScore() {
            float min = Float.MAX_VALUE;
            for (int i = 0; i < minScores.length; i++) {
                if (minScores[i] < min) {
                    min = minScores[i];
                }
            }
            return min;
        }
    }
}
