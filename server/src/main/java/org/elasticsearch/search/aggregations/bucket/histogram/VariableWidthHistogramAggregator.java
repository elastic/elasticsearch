/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.histogram;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.util.CollectionUtil;
import org.apache.lucene.util.InPlaceMergeSorter;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.DoubleArray;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.bucket.BestBucketsDeferringCollector;
import org.elasticsearch.search.aggregations.bucket.DeferableBucketAggregator;
import org.elasticsearch.search.aggregations.bucket.DeferringBucketCollector;
import org.elasticsearch.search.aggregations.bucket.nested.NestedAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.LongUnaryOperator;

public class VariableWidthHistogramAggregator extends DeferableBucketAggregator {

    /**
     * This aggregator goes through multiple phases of collection. Each phase has a different CollectionPhase::collectValue
     * implementation
     *
     * Running a clustering algorithm like K-Means is unfeasible because large indices don't fit into memory.
     * But having multiple collection phases lets us accurately bucket the docs in one pass.
     */
    private abstract static class CollectionPhase implements Releasable {

        /**
         * This method will collect the doc and then either return itself or a new CollectionPhase
         * It is responsible for determining when a phase is over and what phase will run next
         */
        abstract CollectionPhase collectValue(LeafBucketCollector sub, int doc, double val) throws IOException;

        /**
         * @return the final number of buckets that will be used
         * If this is not the final phase, then an instance of the next phase is created and it is asked for this answer.
         */
        abstract int finalNumBuckets();

        /**
         * If this CollectionPhase is the final phase then this method will build and return the i'th bucket
         * Otherwise, it will create an instance of the next phase and ask it for the i'th bucket (naturally, if that phase
         * not the last phase then it will do the same and so on...)
         */
        abstract InternalVariableWidthHistogram.Bucket buildBucket(int bucketOrd, InternalAggregations subAggregations) throws IOException;

    }

    /**
     * Phase 1: Build up a buffer of docs (i.e. give each new doc its own bucket). No clustering decisions are made here.
     * Building this buffer lets us analyze the distribution of the data before we begin clustering.
     */
    private class BufferValuesPhase extends CollectionPhase {

        private DoubleArray buffer;
        private int bufferSize;
        private int bufferLimit;
        private MergeBucketsPhase mergeBucketsPhase;

        BufferValuesPhase(int bufferLimit) {
            this.buffer = bigArrays().newDoubleArray(1);
            this.bufferSize = 0;
            this.bufferLimit = bufferLimit;
            this.mergeBucketsPhase = null;
        }

        @Override
        public CollectionPhase collectValue(LeafBucketCollector sub, int doc, double val) throws IOException {
            if (bufferSize < bufferLimit) {
                // Add to the buffer i.e store the doc in a new bucket
                buffer = bigArrays().grow(buffer, bufferSize + 1);
                buffer.set((long) bufferSize, val);
                collectBucket(sub, doc, bufferSize);
                bufferSize += 1;
            }

            if (bufferSize == bufferLimit) {
                // We have hit the buffer limit. Switch to merge mode
                CollectionPhase mergeBuckets = new MergeBucketsPhase(buffer, bufferSize);
                Releasables.close(this);
                return mergeBuckets;
            } else {
                // There is still room in the buffer
                return this;
            }
        }

        int finalNumBuckets() {
            return getMergeBucketPhase().finalNumBuckets();
        }

        @Override
        InternalVariableWidthHistogram.Bucket buildBucket(int bucketOrd, InternalAggregations subAggregations) throws IOException {
            InternalVariableWidthHistogram.Bucket bucket = getMergeBucketPhase().buildBucket(bucketOrd, subAggregations);
            return bucket;
        }

        MergeBucketsPhase getMergeBucketPhase() {
            if (mergeBucketsPhase == null) {
                mergeBucketsPhase = new MergeBucketsPhase(buffer, bufferSize);
            }
            return mergeBucketsPhase;
        }

        @Override
        public void close() {
            if (mergeBucketsPhase != null) {
                Releasables.close(mergeBucketsPhase);
            }
            Releasables.close(buffer);
        }
    }

    /**
     * Phase 2: This phase is initialized with the buffer created in Phase 1.
     * It is responsible for merging the buffered docs into a smaller number of buckets and then determining which existing
     * bucket all subsequent docs belong to. New buckets will be created for docs that are distant from all existing ones
     */
    private class MergeBucketsPhase extends CollectionPhase {
        /**
         * "Cluster" refers to intermediate buckets during collection
         * They are kept sorted by centroid. The i'th index in all these arrays always refers to the i'th cluster
         */
        public DoubleArray clusterMaxes;
        public DoubleArray clusterMins;
        public DoubleArray clusterCentroids;
        public DoubleArray clusterSizes; // clusterSizes != bucketDocCounts when clusters are in the middle of a merge
        public int numClusters;

        private double avgBucketDistance;

        MergeBucketsPhase(DoubleArray buffer, int bufferSize) {
            // Cluster the documents to reduce the number of buckets
            boolean success = false;
            try {
                bucketBufferedDocs(buffer, bufferSize, mergePhaseInitialBucketCount(shardSize));
                success = true;
            } finally {
                if (success == false) {
                    close();
                    clusterMaxes = clusterMins = clusterCentroids = clusterSizes = null;
                }
            }

            if (bufferSize > 1) {
                updateAvgBucketDistance();
            }
        }

        /**
         * Sorts the <b>indices</b> of <code>values</code> by their underlying value
         * This will produce a merge map whose application will sort <code>values</code>
         */
        private class ClusterSorter extends InPlaceMergeSorter {

            final DoubleArray values;
            final long[] indexes;

            ClusterSorter(DoubleArray values, int length) {
                this.values = values;

                this.indexes = new long[length];
                for (int i = 0; i < indexes.length; i++) {
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
             * See BucketsAggregator::mergeBuckets to learn more about the merge map
             */
            public long[] generateMergeMap() {
                sort(0, indexes.length);
                return indexes;
            }
        }

        /**
         * Sorting the documents by key lets us bucket the documents into groups with a single linear scan
         *
         * But we can't do this by just sorting <code>buffer</code>, because we also need to generate a merge map
         * for every change we make to the list, so that we can apply the changes to the underlying buckets as well.
         *
         * By just creating a merge map, we eliminate the need to actually sort <code>buffer</code>. We can just
         * use the merge map to find any doc's sorted index.
         */
        private void bucketBufferedDocs(final DoubleArray buffer, final int bufferSize, final int numBuckets) {
            // Allocate space for the clusters about to be created
            clusterMins = bigArrays().newDoubleArray(1);
            clusterMaxes = bigArrays().newDoubleArray(1);
            clusterCentroids = bigArrays().newDoubleArray(1);
            clusterSizes = bigArrays().newDoubleArray(1);
            numClusters = 0;

            ClusterSorter sorter = new ClusterSorter(buffer, bufferSize);
            long[] mergeMap = sorter.generateMergeMap();

            // Naively use basic linear separation to group the first bufferSize docs into initialNumBuckets buckets
            // This will require modifying the merge map, which currently represents a sorted list of buckets with 1 doc / bucket
            int docsPerBucket = (int) Math.ceil((double) bufferSize / (double) numBuckets);
            int bucketOrd = 0;
            for (int i = 0; i < mergeMap.length; i++) {
                // mergeMap[i] is the index of the i'th smallest doc
                double val = buffer.get(mergeMap[i]);

                // Put the i'th smallest doc into the bucket at bucketOrd
                mergeMap[i] = (int) (mergeMap[i] / docsPerBucket);
                if (bucketOrd == numClusters) {
                    createAndAppendNewCluster(val);
                } else {
                    addToCluster(bucketOrd, val);
                }

                if ((i + 1) % docsPerBucket == 0) {
                    // This bucket is full. Make a new one
                    bucketOrd += 1;
                }
            }

            LongUnaryOperator howToRewrite = b -> mergeMap[(int) b];
            rewriteBuckets(bucketOrd + 1, howToRewrite);
            if (deferringCollector != null) {
                deferringCollector.rewriteBuckets(howToRewrite);
            }
        }

        @Override
        public CollectionPhase collectValue(LeafBucketCollector sub, int doc, double val) throws IOException {
            int bucketOrd = getNearestBucket(val);
            double distance = Math.abs(clusterCentroids.get(bucketOrd) - val);
            if (bucketOrd == -1 || distance > (2 * avgBucketDistance) && numClusters < shardSize) {
                // Make a new bucket since the document is distant from all existing buckets
                // TODO: (maybe) Create a new bucket for <b>all</b> distant docs and merge down to shardSize buckets at end

                createAndAppendNewCluster(val);
                collectBucket(sub, doc, numClusters - 1);

                if (val > clusterCentroids.get(bucketOrd)) {
                    /*
                     * If the new value is bigger than the nearest bucket then insert
                     * just ahead of bucketOrd so that the array remains sorted.
                     */
                    bucketOrd += 1;
                }
                moveLastCluster(bucketOrd);
                // We've added a new bucket so update the average distance between the buckets
                updateAvgBucketDistance();
            } else {
                addToCluster(bucketOrd, val);
                collectExistingBucket(sub, doc, bucketOrd);
                if (bucketOrd == 0 || bucketOrd == numClusters - 1) {
                    // Only update average distance if the centroid of one of the end buckets is modifed.
                    updateAvgBucketDistance();
                }
            }
            return this;
        }

        private void updateAvgBucketDistance() {
            // Centroids are sorted so the average distance is the difference between the first and last.
            avgBucketDistance = (clusterCentroids.get(numClusters - 1) - clusterCentroids.get(0)) / (numClusters - 1);
        }

        /**
         * Creates a new cluster with  <code>value</code> and appends it to the cluster arrays
         */
        private void createAndAppendNewCluster(double value) {
            // Ensure there is space for the cluster
            clusterMaxes = bigArrays().grow(clusterMaxes, numClusters + 1); // + 1 because indexing starts at 0
            clusterMins = bigArrays().grow(clusterMins, numClusters + 1);
            clusterCentroids = bigArrays().grow(clusterCentroids, numClusters + 1);
            clusterSizes = bigArrays().grow(clusterSizes, numClusters + 1);

            // Initialize the cluster at the end of the array
            clusterMaxes.set(numClusters, value);
            clusterMins.set(numClusters, value);
            clusterCentroids.set(numClusters, value);
            clusterSizes.set(numClusters, 1);

            numClusters += 1;
        }

        /**
         * Move the last cluster to position <code>idx</code>
         * This is expensive because a merge map of size <code>numClusters</code> is created, so don't call this method too often
         *
         * TODO: Make this more efficient
         */
        private void moveLastCluster(int index) {
            if (index != numClusters - 1) {

                // Move the cluster metadata
                double holdMax = clusterMaxes.get(numClusters - 1);
                double holdMin = clusterMins.get(numClusters - 1);
                double holdCentroid = clusterCentroids.get(numClusters - 1);
                double holdSize = clusterSizes.get(numClusters - 1);
                for (int i = numClusters - 1; i > index; i--) {
                    // The clusters in range {index ... numClusters - 1} move up 1 index to make room for the new cluster
                    clusterMaxes.set(i, clusterMaxes.get(i - 1));
                    clusterMins.set(i, clusterMins.get(i - 1));
                    clusterCentroids.set(i, clusterCentroids.get(i - 1));
                    clusterSizes.set(i, clusterSizes.get(i - 1));
                }
                clusterMaxes.set(index, holdMax);
                clusterMins.set(index, holdMin);
                clusterCentroids.set(index, holdCentroid);
                clusterSizes.set(index, holdSize);

                // Move the underlying buckets
                LongUnaryOperator mergeMap = new LongUnaryOperator() {
                    @Override
                    public long applyAsLong(long i) {
                        if (i < index) {
                            // The clusters in range {0 ... idx - 1} don't move
                            return i;
                        }
                        if (i == numClusters - 1) {
                            // The new cluster moves to index
                            return (long) index;
                        }
                        // The clusters in range {index ... numClusters - 1} shift forward
                        return i + 1;
                    }
                };

                rewriteBuckets(numClusters, mergeMap);
                if (deferringCollector != null) {
                    deferringCollector.rewriteBuckets(mergeMap);
                }
            }
        }

        /**
         * Adds <code>val</code> to the cluster at index <code>bucketOrd</code>.
         * The cluster's centroid, min, max, and size are recalculated.
         */
        private void addToCluster(int bucketOrd, double val) {
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
         **/
        private int getNearestBucket(double value) {
            if (numClusters == 0) {
                return -1;
            }
            BigArrays.DoubleBinarySearcher binarySearcher = new BigArrays.DoubleBinarySearcher(clusterCentroids);
            return binarySearcher.search(0, numClusters - 1, value);
        }

        @Override
        int finalNumBuckets() {
            return numClusters;
        }

        @Override
        InternalVariableWidthHistogram.Bucket buildBucket(int bucketOrd, InternalAggregations subAggregations) {
            return new InternalVariableWidthHistogram.Bucket(
                clusterCentroids.get(bucketOrd),
                new InternalVariableWidthHistogram.Bucket.BucketBounds(clusterMins.get(bucketOrd), clusterMaxes.get(bucketOrd)),
                bucketDocCount(bucketOrd),
                formatter,
                subAggregations
            );
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
    private final int bufferLimit;

    private CollectionPhase collector;

    private BestBucketsDeferringCollector deferringCollector;

    VariableWidthHistogramAggregator(
        String name,
        AggregatorFactories factories,
        int numBuckets,
        int shardSize,
        int initialBuffer,
        @Nullable ValuesSourceConfig valuesSourceConfig,
        AggregationContext context,
        Aggregator parent,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, factories, context, parent, metadata);

        this.numBuckets = numBuckets;
        this.valuesSource = (ValuesSource.Numeric) valuesSourceConfig.getValuesSource();
        this.formatter = valuesSourceConfig.format();
        this.shardSize = shardSize;
        this.bufferLimit = initialBuffer;

        collector = new BufferValuesPhase(this.bufferLimit);

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
            throw new IllegalStateException(
                "VariableWidthHistogram agg ["
                    + name()
                    + "] is the child of the nested agg ["
                    + nestedAgg
                    + "], and also has a scoring child agg ["
                    + scoringAgg
                    + "].  This combination is not supported because "
                    + "it requires executing in [depth_first] mode, which the VariableWidthHistogram agg cannot do."
            );
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

    private static String descendsFromNestedAggregator(Aggregator parent) {
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
    public DeferringBucketCollector buildDeferringCollector() {
        deferringCollector = new BestBucketsDeferringCollector(topLevelQuery(), searcher(), descendsFromGlobalAggregator(parent()));
        return deferringCollector;
    }

    @Override
    protected LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) throws IOException {
        if (valuesSource == null) {
            return LeafBucketCollector.NO_OP_COLLECTOR;
        }
        final SortedNumericDoubleValues values = valuesSource.doubleValues(ctx);
        return new LeafBucketCollectorBase(sub, values) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                assert bucket == 0;
                if (values.advanceExact(doc)) {
                    final int valuesCount = values.docValueCount();
                    double prevVal = Double.NEGATIVE_INFINITY;
                    for (int i = 0; i < valuesCount; ++i) {
                        double val = values.nextValue();
                        assert val >= prevVal;
                        if (val == prevVal) {
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
        int numClusters = collector.finalNumBuckets();

        long[] bucketOrdsToCollect = new long[numClusters];
        for (int i = 0; i < numClusters; i++) {
            bucketOrdsToCollect[i] = i;
        }

        InternalAggregations[] subAggregationResults = buildSubAggsForBuckets(bucketOrdsToCollect);

        List<InternalVariableWidthHistogram.Bucket> buckets = new ArrayList<>(numClusters);
        for (int bucketOrd = 0; bucketOrd < numClusters; bucketOrd++) {
            buckets.add(collector.buildBucket(bucketOrd, subAggregationResults[bucketOrd]));
        }

        Function<List<InternalVariableWidthHistogram.Bucket>, InternalAggregation> resultBuilder = bucketsToFormat -> {
            // The contract of the histogram aggregation is that shards must return
            // buckets ordered by centroid in ascending order
            CollectionUtil.introSort(bucketsToFormat, BucketOrder.key(true).comparator());

            InternalVariableWidthHistogram.EmptyBucketInfo emptyBucketInfo = new InternalVariableWidthHistogram.EmptyBucketInfo(
                buildEmptySubAggregations()
            );

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

    public static int mergePhaseInitialBucketCount(int shardSize) {
        // Target shardSizes * (3/4) buckets so that there's room for more distant buckets to be added during rest of collection
        return (int) ((long) shardSize * 3 / 4);
    }
}
