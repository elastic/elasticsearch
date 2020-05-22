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

package org.elasticsearch.search.aggregations.bucket.histogram;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.util.CollectionUtil;
import org.apache.lucene.util.InPlaceMergeSorter;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.DoubleArray;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.bucket.DeferableBucketAggregator;
import org.elasticsearch.search.aggregations.bucket.DeferringBucketCollector;
import org.elasticsearch.search.aggregations.bucket.MergingBucketsDeferringCollector;
import org.elasticsearch.search.aggregations.bucket.nested.NestedAggregator;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class VariableWidthHistogramAggregator extends DeferableBucketAggregator {

    /**
     * This aggregator goes through multiple phases of collection
     *
     * We have to bucket docs as we collect them, in a streaming fashion. Storing all docs and then running a clustering
     * algorithm like K-Means is unfeasible because large indices don't fit into memory.
     */
    private abstract class CollectionPhase implements Releasable {

        /**
         * This method will collect the doc and then either return itself or a new CollectionPhase
         * It is responsible for determining when a phase is over, and what the next phase will be
         */
        abstract CollectionPhase collectValue(LeafBucketCollector sub, int doc, double val) throws IOException;


        /**
         * Produce the amount of buckets that have been used so far
         */
        abstract int numBuckets();

        /**
         * If this CollectionPhase is the final phase then this method will build and return the i'th bucket
         * Otherwise, it will create an instance of the next phase and ask it for the i'th bucket (naturally, if that phase
         * is also not the last phase, it will do the same and so on...)
         */
        abstract InternalVariableWidthHistogram.Bucket buildBucket(int bucketOrd, InternalAggregations subAggregations) throws IOException;

    }

    /**
     * Phase 1: Build up a cache of docs (i.e. give each new doc its own bucket). No clustering decisions are made here
     * Building this cache lets us analyze the distribution of the data before we begin clustering.
     */
    private class CacheValuesPhase extends CollectionPhase{

        private DoubleArray cachedValues;
        private int numCachedDocs;
        private int cacheLimit;

        CacheValuesPhase(int cacheLimit){
            this.cachedValues = bigArrays.newDoubleArray(1);
            this.numCachedDocs = 0;
            this.cacheLimit = cacheLimit;
        }

        @Override
        public CollectionPhase collectValue(LeafBucketCollector sub, int doc, double val) throws IOException{
            if (numCachedDocs < cacheLimit) {
                // Cache the doc in a new bucket
                cachedValues = bigArrays.grow(cachedValues, numCachedDocs + 1);
                cachedValues.set((long) numCachedDocs, val);
                collectBucket(sub, doc, numCachedDocs);
                numCachedDocs += 1;
            }

            if(numCachedDocs == cacheLimit) {
                // We have hit the cache limit. Switch to merge mode
                CollectionPhase mergeBuckets = new MergeBucketsPhase(cachedValues, numCachedDocs);
                Releasables.close(this);
                return mergeBuckets;
            } else {
                // There is still room in the cache
                return this;
            }
        }

        int numBuckets(){
            return numCachedDocs;
        }

        @Override
        InternalVariableWidthHistogram.Bucket buildBucket(int bucketOrd, InternalAggregations subAggregations) throws IOException{
            CollectionPhase mergeBuckets = new MergeBucketsPhase(cachedValues, numCachedDocs);
            InternalVariableWidthHistogram.Bucket bucket = mergeBuckets.buildBucket(bucketOrd, subAggregations);
            Releasables.close(mergeBuckets);
            return bucket;
        }

        @Override
        public void close() {
            Releasables.close(cachedValues);
        }
    }

    /**
     * Phase 2: This phase is initialized with the cache created in Phase 1
     * It is responsible for merging the docs in this cache into a smaller number of buckets and then determining which
     * bucket all subsequent docs belong to. New bucket will be created for docs that are distant from all previous ones
     */
    private class MergeBucketsPhase extends CollectionPhase{
        /**
         * "Cluster" refers to intermediate buckets during collection
         * They are kept sorted by centroid. the i'th index in all these arrays always refers to the i'th cluster
         */
        public DoubleArray clusterMaxes;
        public DoubleArray clusterMins;
        public DoubleArray clusterCentroids;
        public DoubleArray clusterSizes; // clusterSizes.get(i) will not be equal to bucketDocCount(i) when clusters are being merged
        public int numClusters;

        private int avgBucketDistance;

        MergeBucketsPhase(DoubleArray cachedValues, int numCachedDocs) {
            // Cluster the documents to reduce the number of buckets
            // Target is shardSizes * (3/4) so that there's room for more distant buckets to be added during rest of collection
            bucketCachedDocs(cachedValues, numCachedDocs, shardSize * 3 / 4);

            // Calculate the average distance between buckets
            // Subsequent documents will be compared with this value to determine if they should be collected into
            // an existing bucket or into a new bucket
            // This can be done in a single linear scan because buckets are sorted by centroid
            int sum = 0;
            for (int i = 0; i < numClusters - 1; i++) {
                sum += clusterCentroids.get(i + 1) - clusterCentroids.get(i);
            }
            avgBucketDistance = (sum / (numClusters - 1));
        }

        /**
         * Sorts the <b>indices</b> of <code>values</code> by their underlying value
         * This will produce a merge map whose application will sort <code>values</code>
         */
        private class ClusterIndexSorter extends InPlaceMergeSorter {

            final DoubleArray values;
            final long[] indexes;
            int length;

            ClusterIndexSorter(DoubleArray values, int length){
                this.values = values;
                this.length = length;

                this.indexes = new long[length];
                for(int i = 0; i < indexes.length; i++){
                    indexes[i] = i;
                }
            }

            @Override
            protected int compare(int i, int j) {
                double iVal = values.get(indexes[i]);
                double jVal = values.get(indexes[j]);
                return Double.compare(iVal, jVal);
            }

            @Override
            protected void swap(int i, int j) {
                long hold = indexes[i];
                indexes[i] = indexes[j];
                indexes[j] = hold;
            }

            /**
             * Produces a merge map where `mergeMap[i]` represents the index that <code>values[i]</code>
             * would be moved to <b>if</b> <code>values</code> were sorted
             * In other words, this method produces a merge map that will sort <code>values</code>
             *
             * See <code>BucketsAggregator::mergeBuckets</code> to learn more about the merge map
             */
            public long[] generateMergeMap(){
                sort(numClusters - 1, 0);
                return indexes;
            }
        }

        /**
         * Sorting the documents by key lets us do bucketing with a single linear scan
         *
         * But we can't just sort <code>cachedValues</code> because we also need to generate a merge map for every change
         * we make to the list, so that we can apply them to the underlying buckets as well
         *
         * If we create a merge map then we don't ever need to actually sort <code>cachedValues</code> because
         * we can use the merge map to find any doc's sorted index
         */
        private void bucketCachedDocs(final DoubleArray cachedValues, final int numCachedDocs, final int numBuckets){
            // Allocate space for the clusters about to be created
            clusterMins = bigArrays.newDoubleArray(1);
            clusterMaxes = bigArrays.newDoubleArray(1);
            clusterCentroids = bigArrays.newDoubleArray(1);
            clusterSizes = bigArrays.newDoubleArray(1);
            numClusters = 0;

            ClusterIndexSorter sorter = new ClusterIndexSorter(cachedValues, numCachedDocs);
            long[] mergeMap = sorter.generateMergeMap();

            // Naively use basic linear separation to group the first cacheLimit docs into initialNumBuckets buckets
            // This will require modifying the merge map, which currently represents a sorted list of buckets with 1 doc / bucket
            int docsPerBucket = (int) Math.ceil((double) numCachedDocs / (double) numBuckets);
            int bucketOrd = 0;
            for(int i = 0; i < mergeMap.length; i++){
                // mergeMap[i] is the index of the i'th smallest doc
                double val = cachedValues.get(mergeMap[i]);

                // Put the i'th smallest doc into bucket bucketOrd
                mergeMap[i] = bucketOrd;
                if(bucketOrd == numClusters){
                    createAndAppendNewCluster(val);
                } else {
                    addToCluster(bucketOrd, val);
                }

                if((i + 1) % docsPerBucket == 0){
                    // This bucket is full. Make a new one
                    bucketOrd += 1;
                }
            }

            mergeBuckets(mergeMap, numBuckets);
            if (deferringCollector != null) {
                deferringCollector.mergeBuckets(mergeMap);
            }
        }

        @Override
        public CollectionPhase collectValue(LeafBucketCollector sub, int doc, double val) throws IOException{
            int bucketOrd = getNearestBucket(val);
            double distance = Math.abs(clusterCentroids.get(bucketOrd)- val);
            if(bucketOrd == -1 || distance > (2 * avgBucketDistance) && numClusters < shardSize) {
                // Make a new bucket since the document is distant from all existing buckets
                // TODO: (maybe) Create a new bucket for all distant docs and merge down to `shardSize` buckets at end

                createAndAppendNewCluster(val);
                collectBucket(sub, doc, numClusters - 1);
                if(val > clusterCentroids.get(bucketOrd)){
                    // Insert just ahead of bucketOrd so that the array remains sorted
                    bucketOrd += 1;
                }
                moveLastCluster(bucketOrd);
            } else {
                addToCluster(bucketOrd, val);
                collectExistingBucket(sub, doc, bucketOrd);
            }
            return this;
        }

        /***
         * Creates a new cluster with  <code>var</code> at the end of the cluster arrays
         */
        private void createAndAppendNewCluster(double val){
            // Ensure there is space for the cluster
            clusterMaxes = bigArrays.grow(clusterMaxes, numClusters + 1); //  + 1 because indexing starts at 0
            clusterMins = bigArrays.grow(clusterMins, numClusters + 1);
            clusterCentroids = bigArrays.grow(clusterCentroids, numClusters + 1);
            clusterSizes = bigArrays.grow(clusterSizes, numClusters + 1);

            // Initialize the cluster at the end of the array
            clusterMaxes.set(numClusters, val);
            clusterMins.set(numClusters, val);
            clusterCentroids.set(numClusters, val);
            clusterSizes.set(numClusters, 1);

            numClusters += 1;
        }
        
        /**
         * Move the last cluster to position <code>idx</code>
         * This is expensive because a merge map of size <code>numClusters</code> is created, so don't do this often
         *
         * TODO: Make this more efficient
         */
        private void moveLastCluster(int idx){
            if(idx != numClusters - 1) {

                // Move the cluster metadata
                double holdMax = clusterMaxes.get(numClusters-1);
                double holdMin = clusterMins.get(numClusters-1);
                double holdCentroid = clusterCentroids.get(numClusters-1);
                double holdSize = clusterSizes.get(numClusters-1);
                for (int i = numClusters - 1; i > idx; i--) {
                    // The clusters in range {index ... numClusters - 1} move up 1 index to make room for the new cluster
                    clusterMaxes.set(i, clusterMaxes.get(i-1));
                    clusterMins.set(i, clusterMins.get(i-1));
                    clusterCentroids.set(i, clusterCentroids.get(i-1));
                    clusterSizes.set(i, clusterSizes.get(i-1));
                }
                clusterMaxes.set(idx, holdMax);
                clusterMins.set(idx, holdMin);
                clusterCentroids.set(idx, holdCentroid);
                clusterSizes.set(idx, holdSize);

                // Move the underlying buckets
                long[] mergeMap = new long[numClusters];
                for (int i = 0; i < idx; i++) {
                    // The clusters in range {0 ... idx - 1} don't move
                    mergeMap[i] = i;
                }
                for (int i = idx; i < numClusters - 1; i++) {
                    // The clusters in range {index ... numClusters - 1} shift up
                    mergeMap[i] = i + 1;
                }
                // Finally, the new cluster moves to index
                mergeMap[numClusters - 1] = idx;

                // TODO: Create a moveLastCluster() method that is like mergeBuckets but doesn't require a merge map
                //       This would be more efficient - there would be no need to create a merge map every time
                mergeBuckets(mergeMap, numClusters);
                if (deferringCollector != null) {
                    deferringCollector.mergeBuckets(mergeMap);
                }
            }
        }

        /**
         * Add <code>val</code> to the cluster at index <code>bucketOrd</code>.
         * The cluster's centroid, min, max, and size are recalculated.
         */
        private void addToCluster(int bucketOrd, double val){
            assert bucketOrd < numClusters;

            double max = Math.max(clusterMaxes.get(bucketOrd), val);
            double min = Math.min(clusterMins.get(bucketOrd), val);

            // Recalculate the centroid
            double oldCentroid = clusterCentroids.get(bucketOrd);
            double size = clusterSizes.get(bucketOrd);
            double newCentroid = ((oldCentroid * size) + val) / (size + 1);

            clusterMaxes.set(bucketOrd, max);
            clusterMins.set(bucketOrd, min);
            clusterCentroids.set(bucketOrd, newCentroid);
            clusterSizes.increment(bucketOrd, 1);
        }

        /**
         * Returns the ordinal of the bucket whose centroid is closest to <code>val</code>, or -1 if there are no buckets.
         *
         * This is done using a binary search, since <code>clusterCentroids</code> is always kept sorted.
         * But, we don't care if <code>val</code> matches a bucket's key exactly. We just want the nearest bucket,
         * and this requires some extra logic.
         */
        private int getNearestBucket(double val) {
            if(numClusters == 0) return -1;

            int left = 0;
            int right = numClusters - 1;
            while(left < right){
                int mid = (left + right) / 2;
                double midVal = clusterCentroids.get(mid);

                if(midVal == val){
                    return mid;
                } else if(midVal < val){
                    if(mid < numClusters - 1) {
                        double valAfterMid = clusterCentroids.get(mid + 1);
                        if (val < valAfterMid) {
                            // val is between indices mid, (mid + 1)
                            if (valAfterMid - val < val - midVal) {
                                return mid + 1;
                            } else {
                                return mid;
                            }
                        }
                    } else if(mid == numClusters - 1){
                        // There are are no more clusters above mid so mid is the closest
                        return mid;
                    }

                    left = mid + 1;
                } else{ // midVal > val
                    // Check if mid is the closest element. This is still possible because val is a double
                    if(mid > 0) {
                        double valBeforeMid = clusterCentroids.get(mid - 1);
                        if (val > valBeforeMid) {
                            // val is between indices (mid - 1), mid
                            if (midVal - val < val - valBeforeMid) {
                                return mid;
                            } else {
                                return mid - 1;
                            }
                        }
                    } else if(mid == 0){
                        // There are no more clusters below mid, so mid is the closest
                        return mid;
                    }

                    right = mid - 1;
                }
            }

            return left;
        }

        int numBuckets(){
            return numClusters;
        }

        @Override
        InternalVariableWidthHistogram.Bucket buildBucket(int bucketOrd, InternalAggregations subAggregations) throws IOException{
             return new InternalVariableWidthHistogram.Bucket(
                    clusterCentroids.get(bucketOrd),
                    new InternalVariableWidthHistogram.Bucket.BucketBounds(clusterMins.get(bucketOrd), clusterMaxes.get(bucketOrd)),
                    bucketDocCount(bucketOrd),
                    formatter,
                    subAggregations);
        }

        @Override
        public void close() {
            Releasables.close(clusterMaxes, clusterMins, clusterCentroids, clusterSizes);
        }
    }

    private final ValuesSource.Numeric valuesSource;
    private final DocValueFormat formatter;

    // Aggregation parameters
    private final int numBuckets;
    private final int shardSize;
    private final int cacheLimit;

    final BigArrays bigArrays;
    CollectionPhase collector;

    private MergingBucketsDeferringCollector deferringCollector;

    VariableWidthHistogramAggregator(String name, AggregatorFactories factories, int numBuckets, int shardSize,
                                     int cacheLimit, @Nullable ValuesSource valuesSource,
                                     DocValueFormat formatter, SearchContext context, Aggregator parent,
                                     Map<String, Object> metadata) throws IOException{
        super(name, factories, context, parent, metadata);

        this.numBuckets = numBuckets;
        this.valuesSource = (ValuesSource.Numeric) valuesSource;
        this.formatter = formatter;
        this.shardSize = shardSize;
        this.cacheLimit = cacheLimit;

        bigArrays = context.bigArrays();
        collector = new CacheValuesPhase(this.cacheLimit);

        String scoringAgg = subAggsNeedScore();
        String nestedAgg = descendsFromNestedAggregator(parent);
        if (scoringAgg != null && nestedAgg != null) {
            /*
             * Terms agg would force the collect mode to depth_first here, because
             * we need to access the score of nested documents in a sub-aggregation
             * and we are not able to generate this score while replaying deferred documents.
             *
             * But the VariableWidthHistogram agg _must_ execute in breadth first since it relies on
             * deferring execution, so we just have to throw up our hands and refuse
            */
            throw new IllegalStateException("VariableWidthHistogram agg [" + name() + "] is the child of the nested agg [" + nestedAgg
                + "], and also has a scoring child agg [" + scoringAgg + "].  This combination is not supported because " +
                "it requires executing in [depth_first] mode, which the VariableWidthHistogram agg cannot do.");
        }
    }

    private String subAggsNeedScore() {
        for (Aggregator subAgg : subAggregators) {
            if (subAgg.scoreMode().needsScores()) {
                return subAgg.name();
            }
        }
        return null;
    }

    private String descendsFromNestedAggregator(Aggregator parent) {
        while (parent != null) {
            if (parent.getClass() == NestedAggregator.class) {
                return parent.name();
            }
            parent = parent.parent();
        }
        return null;
    }

    @Override
    public ScoreMode scoreMode() {
        if (valuesSource != null && valuesSource.needsScores()) {
            return ScoreMode.COMPLETE;
        }
        return super.scoreMode();
    }

    @Override
    protected boolean shouldDefer(Aggregator aggregator) {
        return true;
    }

    @Override
    public DeferringBucketCollector getDeferringCollector() {
        deferringCollector = new MergingBucketsDeferringCollector(context, descendsFromGlobalAggregator(parent()));
        return deferringCollector;
    }

    @Override
    protected LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) throws IOException {
        if (valuesSource == null) {
            return LeafBucketCollector.NO_OP_COLLECTOR;
        }
        final SortedNumericDoubleValues values = valuesSource.doubleValues(ctx);
        return new LeafBucketCollectorBase(sub, values){
            @Override
            public void collect(int doc, long bucket) throws IOException {
                assert bucket == 0;
                if(values.advanceExact(doc)){
                    final int valuesCount = values.docValueCount();
                    double prevVal = Double.NEGATIVE_INFINITY;
                    for (int i = 0; i < valuesCount; ++i) {
                        double val = values.nextValue();
                        assert val >= prevVal;
                        if (val == prevVal){
                            continue;
                        }

                        collector = collector.collectValue(sub, doc, val);
                    }
                }
            }
        };
    }


    @Override
    public InternalAggregation[] buildAggregations(long[] owningBucketOrds) throws IOException {
        int numClusters = collector.numBuckets();

        consumeBucketsAndMaybeBreak(numClusters);

        long[] bucketOrdsToCollect = new long[numClusters];
        for (int i = 0; i < numClusters; i++) {
            bucketOrdsToCollect[i] = i;
        }

        InternalAggregations[] subAggregationResults = buildSubAggsForBuckets(bucketOrdsToCollect);

        List<InternalVariableWidthHistogram.Bucket> buckets = new ArrayList<>(numClusters);
        for (int bucketOrd = 0; bucketOrd < numClusters; bucketOrd++) {
            buckets.add(collector.buildBucket(bucketOrd, subAggregationResults[bucketOrd]));
        }

        Function<List<InternalVariableWidthHistogram.Bucket>, InternalAggregation> resultBuilder =  bucketsToFormat -> {
            // The contract of the histogram aggregation is that shards must return
            // buckets ordered by centroid in ascending order
            CollectionUtil.introSort(bucketsToFormat, BucketOrder.key(true).comparator());

            InternalVariableWidthHistogram.EmptyBucketInfo emptyBucketInfo = new InternalVariableWidthHistogram.EmptyBucketInfo(
                buildEmptySubAggregations());

            return new InternalVariableWidthHistogram(name, bucketsToFormat, emptyBucketInfo, numBuckets, formatter, metadata());
        };

        return new InternalAggregation[] { resultBuilder.apply(buckets) };

    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        InternalVariableWidthHistogram.EmptyBucketInfo emptyBucketInfo = new InternalVariableWidthHistogram.EmptyBucketInfo(
            buildEmptySubAggregations()
        );
        return new InternalVariableWidthHistogram(name(), Collections.emptyList(), emptyBucketInfo, numBuckets, formatter, metadata());
    }

    @Override
    public void doClose() {
        Releasables.close(collector);
    }

}
