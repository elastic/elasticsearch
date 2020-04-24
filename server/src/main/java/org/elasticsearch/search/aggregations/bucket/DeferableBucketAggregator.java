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

package org.elasticsearch.search.aggregations.bucket;

import org.apache.lucene.util.IOSupplier;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.LongHash;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory.MultiBucketAggregatorWrapper;
import org.elasticsearch.search.aggregations.BucketCollector;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.MultiBucketCollector;
import org.elasticsearch.search.aggregations.bucket.global.GlobalAggregator;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public abstract class DeferableBucketAggregator extends BucketsAggregator {
    /**
     * Wrapper that records collections. Non-null if any aggregations have
     * been deferred.
     */
    private DeferringBucketCollector recordingWrapper;
    /**
     * Hash of surviving sub-aggregation ordinals. Non-null if there are any
     * surviving ordinals.
     */
    private LongHash survivingOrds;
    /**
     * List of deferred results to build when running deferred collections.
     * Non-null if any results have been deferred.
     * <p>
     * Its kind of a shame to keep a {@linkplain List} around to keep the
     * reference to these but we must build the results <strong>after</strong>
     * running our deferred collections and <strong>before</strong> running our
     * sub-aggregation's deferred collections. This is because it is building
     * our results that marks which gives our sub-aggregators the chance to
     * mark any of their buckets as surviving.
     */
    private List<DeferredInternalAggregation> deferred;

    protected DeferableBucketAggregator(String name, AggregatorFactories factories, SearchContext context, Aggregator parent,
            Map<String, Object> metadata) throws IOException {
        super(name, factories, context, parent, metadata);
    }

    @Override
    protected void doPreCollection() throws IOException {
        List<BucketCollector> collectors = new ArrayList<>();
        List<BucketCollector> deferredCollectors = new ArrayList<>();
        for (int i = 0; i < subAggregators.length; ++i) {
            if (shouldDefer(subAggregators[i])) {
                if (recordingWrapper == null) {
                    recordingWrapper = getDeferringCollector();
                }
                deferredCollectors.add(subAggregators[i]);
                subAggregators[i] = recordingWrapper.wrap(subAggregators[i]);
            } else {
                collectors.add(subAggregators[i]);
            }
        }
        if (recordingWrapper != null) {
            recordingWrapper.setDeferredCollector(deferredCollectors);
            collectors.add(recordingWrapper);
        }
        collectableSubAggregators = MultiBucketCollector.wrap(collectors);
    }

    public static boolean descendsFromGlobalAggregator(Aggregator parent) {
        while (parent != null) {
            if (parent.getClass() == GlobalAggregator.class) {
                return true;
            }
            parent = parent.parent();
        }
        return false;
    }

    public DeferringBucketCollector getDeferringCollector() {
        // Default impl is a collector that selects the best buckets
        // but an alternative defer policy may be based on best docs.
        return new BestBucketsDeferringCollector(context(), descendsFromGlobalAggregator(parent()));
    }

    /**
     * This method should be overridden by subclasses that want to defer
     * calculation of a child aggregation until a first pass is complete and a
     * set of buckets has been pruned. Deferring collection will require the
     * recording of all doc/bucketIds from the first pass and then the sub class
     * should call {@link #runDeferredCollections(long...)} for the selected set
     * of buckets that survive the pruning.
     *
     * @param aggregator
     *            the child aggregator
     * @return true if the aggregator should be deferred until a first pass at
     *         collection has completed
     */
    protected boolean shouldDefer(Aggregator aggregator) {
        return false;
    }

    /**
     * Collect sub aggregations for a list of bucket ordinals. This may
     * only be called once so any aggregation that calls this must be
     * wrapped in {@link MultiBucketAggregatorWrapper}.
     * @deprecated prefer delaying construction of the result with many calls
     *             to {@link #recordSurvingOrd(long)} and returning
     *             {@link #deferred(IOSupplier)}.  
     */
    @Deprecated
    protected final void runDeferredCollections(long... bucketOrds) throws IOException {
        // Being lenient here - ignore calls where there are no deferred
        // collections to playback
        if (recordingWrapper != null) {
            recordingWrapper.replay(bucketOrds);
        }
    }

    @Override
    public final boolean runDeferredCollections() throws IOException {
        if (survivingOrds == null) {
            /*
             * If there aren't any ords that need to be replayed we just
             * function as a regular aggregator, giving our sub-aggregation's
             * a chance to run deferred collections if they need to.
             */
            return super.runDeferredCollections();
        }
        /*
         * If we have any deferred collections we reply them and then
         * build our results. Building our results will have the side
         * effect of marking any of the our children's surviving buckets
         * for replay. So, after that, we can finally run our children's
         * deferred collections by calling super.runDeferredCollections.
         */
        recordingWrapper.prepareSelectedBuckets(survivingOrds);
        for (DeferredInternalAggregation d : deferred) {
            d.buildRealResult();
        }
        super.runDeferredCollections();
        return true;
    }

    /**
     * Record an ordinal to be replayed before suppliers passed to
     * {@link #deferred(IOSupplier)} are called.
     */
    protected final void recordSurvingOrd(long ord) {
        if (recordingWrapper == null) {
            /*
             * recording wrapper is null if no child aggregations have
             * been delayed.
             */
            return;
        }
        if (survivingOrds == null) {
            survivingOrds = new LongHash(1, context.bigArrays());
        }
        survivingOrds.add(ord);
    }

    /**
     * Build a "deferred" aggregation result. The provided supplier is called
     * after all ordinals recorded with {@link #recordSurvingOrd(long)} are
     * replayed on child aggregations.
     */
    protected final InternalAggregation deferred(IOSupplier<InternalAggregation> buildRealResult) throws IOException {
        if (survivingOrds == null) {
            /*
             * No aggregations have been delayed so we are safe to finish
             * building the result right away. 
             */
            return buildRealResult.get();
        }
        if (deferred == null) {
            deferred = new ArrayList<>();
        }
        DeferredInternalAggregation d = new DeferredInternalAggregation(name, buildRealResult);
        deferred.add(d);
        return d;
    }

    @Override
    protected void doClose() {
        super.doClose();
        Releasables.close(survivingOrds);
    }

    private static class DeferredInternalAggregation extends InternalAggregation {
        private final IOSupplier<InternalAggregation> buildRealResult;
        private InternalAggregation realResult;

        DeferredInternalAggregation(String name, IOSupplier<InternalAggregation> buildRealResult) {
            super(name, null);
            this.buildRealResult = buildRealResult;
        }

        @Override
        public String toString() {
            if (realResult == null) {
                return "deferred [" + name + "/" + buildRealResult + "]";
            } else {
                return realResult.toString();
            }
        }

        private void buildRealResult() throws IOException {
            realResult = buildRealResult.get();
            if (realResult == null) {
                throw new IllegalStateException("undeferring bucket didn't build anything");
            }
        }

        @Override
        public InternalAggregation undefer() {
            if (realResult == null) {
                throw new IllegalStateException("deferred collections not replayed!");
            }
            return realResult.undefer();
        }

        @Override
        public String getWriteableName() {
            throw new IllegalStateException();
        }

        @Override
        protected void doWriteTo(StreamOutput out) throws IOException {
            throw new IllegalStateException();
        }

        @Override
        public InternalAggregation reduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
            throw new IllegalStateException();
        }

        @Override
        public Object getProperty(List<String> path) {
            throw new IllegalStateException();
        }

        @Override
        public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
            throw new IllegalStateException();
        }
    }
}
