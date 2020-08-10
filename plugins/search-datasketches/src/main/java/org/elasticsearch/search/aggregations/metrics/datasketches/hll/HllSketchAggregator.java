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
package org.elasticsearch.search.aggregations.metrics.datasketches.hll;

import org.apache.datasketches.hll.HllSketch;
import org.apache.datasketches.hll.TgtHllType;
import org.apache.datasketches.hll.Union;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregator;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

public class HllSketchAggregator extends NumericMetricsAggregator.SingleValue {
    private final int defaultLgk;
    private final ValuesSource valuesSource;
    private ObjectArray<Union> valueSketchUnions;

    /**
     * constructor of HllSketchAggregator
     * @param name name of this Aggregator
     * @param valuesSourceConfig config about value source
     * @param defaultLgk default lgK when any hll union construct
     * @param context context about query
     * @param parent parent of this Aggregator
     * @param metadata metadata of this Aggregator
     * @throws IOException throws by it's super class
     */
    protected HllSketchAggregator(
        String name,
        ValuesSourceConfig valuesSourceConfig,
        int defaultLgk,
        SearchContext context,
        Aggregator parent,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, context, parent, metadata);
        this.valuesSource = valuesSourceConfig.hasValues() ? valuesSourceConfig.getValuesSource() : null;
        this.defaultLgk = defaultLgk;
        this.valueSketchUnions = context.bigArrays().newObjectArray(1);
    }

    public boolean needsScores() {
        return this.valuesSource != null && this.valuesSource.needsScores();
    }

    private boolean hasDataForBucket(long bucketOrd) {
        return bucketOrd < valueSketchUnions.size() && valueSketchUnions.get(bucketOrd) != null;
    }

    /**
     * get metric of this HllSketchAggregator
     * @param owningBucketOrd bucket order
     * @return metric of this Aggregator
     */
    @Override
    public double metric(long owningBucketOrd) {
        return hasDataForBucket(owningBucketOrd) ? 0.0D : this.valueSketchUnions.get(owningBucketOrd).getEstimate();
    }

    @Override
    public InternalAggregation buildAggregation(long bucket) throws IOException {
        if (hasDataForBucket(bucket)) {
            final HllSketch valueSketch = valueSketchUnions.get(bucket).getResult(TgtHllType.HLL_8);
            return new InternalHllSketchAggregation(name, valueSketch, this.metadata());
        } else {
            return buildEmptyAggregation();
        }
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalHllSketchAggregation(name, new HllSketch(defaultLgk, TgtHllType.HLL_8), this.metadata());
    }

    /**
     * get LeafBucketCollector of this Aggregator
     * @param ctx context of Reader
     * @param sub sub collector
     * @return LeafBucketCollector of this Aggregator
     * @throws IOException this function actually will not throws IOException.
     */
    @Override
    protected LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) throws IOException {

        if (valuesSource == null) {
            return LeafBucketCollector.NO_OP_COLLECTOR;
        }

        final BigArrays bigArrays = context.bigArrays();
        final SortedBinaryDocValues values = this.valuesSource.bytesValues(ctx);

        return new LeafBucketCollectorBase(sub, values) {
            @Override
            public void collect(int doc, long bucket) throws IOException {

                if (valueSketchUnions.size() <= bucket) {
                    valueSketchUnions = bigArrays.grow(valueSketchUnions, bucket + 1);
                }
                Union valueSketchUnion = valueSketchUnions.get(bucket);

                if (values.advanceExact(doc)) {
                    final int valueCount = values.docValueCount();
                    for (int i = 0; i < valueCount; i++) {
                        final BytesRef bytesRef = values.nextValue();
                        final byte[] arr = Arrays.copyOfRange(bytesRef.bytes, bytesRef.offset, bytesRef.length + bytesRef.offset);
                        try {
                            if (valueSketchUnion == null) {
                                valueSketchUnion = Union.heapify(arr);
                            } else {
                                valueSketchUnion.update(HllSketch.heapify(arr));
                            }
                        } catch (Exception e) {
                            // convert to IOException
                            throw new IOException("HllSketch Aggregator Collector error happened", e);
                        }
                    }
                }
                // put result to valueSketches
                if (valueSketchUnion == null) {
                    valueSketchUnion = new Union(defaultLgk);
                }
                valueSketchUnions.set(bucket, valueSketchUnion);
            }
        };
    }

    /**
     * release resources when close
     */
    protected void doClose() {
        Releasables.close(new Releasable[] { this.valueSketchUnions });
    }

}
