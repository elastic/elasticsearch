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

package org.elasticsearch.search.aggregations;

import com.google.common.collect.Iterables;
import org.apache.lucene.index.AtomicReaderContext;
import org.elasticsearch.common.lucene.ReaderContextAware;
import org.elasticsearch.search.aggregations.Aggregator.BucketAggregationMode;

import java.io.IOException;

/**
 * A Collector that can collect data in separate buckets.
 */
public abstract class BucketCollector implements ReaderContextAware {
    
    /**
     * Used to gather a summary from a bucket
     */
    public interface BucketAnalysisCollector{
        /**
         * Used to ask {@link BucketCollector}s for their analysis of the content collected in a bucket
         * @param analysis an object that represents the summary of a bucket as an {@link Aggregation}
         */
        void add(Aggregation aggregation);
    }

    public final static BucketCollector NO_OP_COLLECTOR = new BucketCollector() {

        @Override
        public void collect(int docId, long bucketOrdinal) throws IOException {
            // no-op
        }
        @Override
        public void setNextReader(AtomicReaderContext reader) {
            // no-op
        }
        @Override
        public void postCollection() throws IOException {
            // no-op
        }
        @Override
        public void gatherAnalysis(BucketAnalysisCollector analysisCollector, long bucketOrdinal) {
            // no-op
        }
    };

    /**
     * Wrap the given collectors into a single instance.
     */
    public static BucketCollector wrap(Iterable<? extends BucketCollector> collectorList) {
        final BucketCollector[] collectors = Iterables.toArray(collectorList, BucketCollector.class);
        switch (collectors.length) {
            case 0:
                return NO_OP_COLLECTOR;
            case 1:
                return collectors[0];
            default:
                return new BucketCollector() {

                    @Override
                    public void collect(int docId, long bucketOrdinal) throws IOException {
                        for (BucketCollector collector : collectors) {
                            collector.collect(docId, bucketOrdinal);
                        }
                    }

                    @Override
                    public void setNextReader(AtomicReaderContext reader) {
                        for (BucketCollector collector : collectors) {
                            collector.setNextReader(reader);
                        }
                    }

                    @Override
                    public void postCollection() throws IOException {
                        for (BucketCollector collector : collectors) {
                            collector.postCollection();
                        }
                    }

                    @Override
                    public void gatherAnalysis(BucketAnalysisCollector results, long bucketOrdinal) {
                        for (BucketCollector collector : collectors) {
                            collector.gatherAnalysis(results, bucketOrdinal);
                        }
                    }

                };
        }
    }

    /**
     * Called during the query phase, to collect & aggregate the given document.
     *
     * @param doc                   The document to be collected/aggregated
     * @param bucketOrdinal         The ordinal of the bucket this aggregator belongs to, assuming this aggregator is not a top level aggregator.
     *                              Typically, aggregators with {@code #bucketAggregationMode} set to {@link BucketAggregationMode#MULTI_BUCKETS}
     *                              will heavily depend on this ordinal. Other aggregators may or may not use it and can see this ordinal as just
     *                              an extra information for the aggregation context. For top level aggregators, the ordinal will always be
     *                              equal to 0.
     * @throws IOException
     */
    public abstract void collect(int docId, long bucketOrdinal) throws IOException;

    /**
     * Post collection callback.
     */
    public abstract void postCollection() throws IOException;

    /**
     * Called post-collection to gather the results from surviving buckets.
     * @param analysisCollector
     * @param bucketOrdinal
     */
    public abstract void gatherAnalysis(BucketAnalysisCollector analysisCollector, long bucketOrdinal);
}
