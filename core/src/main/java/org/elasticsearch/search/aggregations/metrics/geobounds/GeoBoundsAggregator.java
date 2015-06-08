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

package org.elasticsearch.search.aggregations.metrics.geobounds;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.DoubleArray;
import org.elasticsearch.index.fielddata.MultiGeoPointValues;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.metrics.MetricsAggregator;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public final class GeoBoundsAggregator extends MetricsAggregator {

    private final ValuesSource.GeoPoint valuesSource;
    private final boolean wrapLongitude;
    DoubleArray tops;
    DoubleArray bottoms;
    DoubleArray posLefts;
    DoubleArray posRights;
    DoubleArray negLefts;
    DoubleArray negRights;

    protected GeoBoundsAggregator(String name, AggregationContext aggregationContext, Aggregator parent,
            ValuesSource.GeoPoint valuesSource, boolean wrapLongitude, List<PipelineAggregator> pipelineAggregators,
            Map<String, Object> metaData) throws IOException {
        super(name, aggregationContext, parent, pipelineAggregators, metaData);
        this.valuesSource = valuesSource;
        this.wrapLongitude = wrapLongitude;
        if (valuesSource != null) {
            final BigArrays bigArrays = context.bigArrays();
            tops = bigArrays.newDoubleArray(1, false);
            tops.fill(0, tops.size(), Double.NEGATIVE_INFINITY);
            bottoms = bigArrays.newDoubleArray(1, false);
            bottoms.fill(0, bottoms.size(), Double.POSITIVE_INFINITY);
            posLefts = bigArrays.newDoubleArray(1, false);
            posLefts.fill(0, posLefts.size(), Double.POSITIVE_INFINITY);
            posRights = bigArrays.newDoubleArray(1, false);
            posRights.fill(0, posRights.size(), Double.NEGATIVE_INFINITY);
            negLefts = bigArrays.newDoubleArray(1, false);
            negLefts.fill(0, negLefts.size(), Double.POSITIVE_INFINITY);
            negRights = bigArrays.newDoubleArray(1, false);
            negRights.fill(0, negRights.size(), Double.NEGATIVE_INFINITY);
        }
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx,
            LeafBucketCollector sub) {
        if (valuesSource == null) {
            return LeafBucketCollector.NO_OP_COLLECTOR;
        }
        final BigArrays bigArrays = context.bigArrays();
        final MultiGeoPointValues values = valuesSource.geoPointValues(ctx);
        return new LeafBucketCollectorBase(sub, values) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                if (bucket >= tops.size()) {
                    long from = tops.size();
                    tops = bigArrays.grow(tops, bucket + 1);
                    tops.fill(from, tops.size(), Double.NEGATIVE_INFINITY);
                    bottoms = bigArrays.resize(bottoms, tops.size());
                    bottoms.fill(from, bottoms.size(), Double.POSITIVE_INFINITY);
                    posLefts = bigArrays.resize(posLefts, tops.size());
                    posLefts.fill(from, posLefts.size(), Double.POSITIVE_INFINITY);
                    posRights = bigArrays.resize(posRights, tops.size());
                    posRights.fill(from, posRights.size(), Double.NEGATIVE_INFINITY);
                    negLefts = bigArrays.resize(negLefts, tops.size());
                    negLefts.fill(from, negLefts.size(), Double.POSITIVE_INFINITY);
                    negRights = bigArrays.resize(negRights, tops.size());
                    negRights.fill(from, negRights.size(), Double.NEGATIVE_INFINITY);
                }

                values.setDocument(doc);
                final int valuesCount = values.count();

                for (int i = 0; i < valuesCount; ++i) {
                    GeoPoint value = values.valueAt(i);
                    double top = tops.get(bucket);
                    if (value.lat() > top) {
                        top = value.lat();
                    }
                    double bottom = bottoms.get(bucket);
                    if (value.lat() < bottom) {
                        bottom = value.lat();
                    }
                    double posLeft = posLefts.get(bucket);
                    if (value.lon() >= 0 && value.lon() < posLeft) {
                        posLeft = value.lon();
                    }
                    double posRight = posRights.get(bucket);
                    if (value.lon() >= 0 && value.lon() > posRight) {
                        posRight = value.lon();
                    }
                    double negLeft = negLefts.get(bucket);
                    if (value.lon() < 0 && value.lon() < negLeft) {
                        negLeft = value.lon();
                    }
                    double negRight = negRights.get(bucket);
                    if (value.lon() < 0 && value.lon() > negRight) {
                        negRight = value.lon();
                    }
                    tops.set(bucket, top);
                    bottoms.set(bucket, bottom);
                    posLefts.set(bucket, posLeft);
                    posRights.set(bucket, posRight);
                    negLefts.set(bucket, negLeft);
                    negRights.set(bucket, negRight);
                }
            }
        };
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) {
        if (valuesSource == null) {
            return buildEmptyAggregation();
        }
        double top = tops.get(owningBucketOrdinal);
        double bottom = bottoms.get(owningBucketOrdinal);
        double posLeft = posLefts.get(owningBucketOrdinal);
        double posRight = posRights.get(owningBucketOrdinal);
        double negLeft = negLefts.get(owningBucketOrdinal);
        double negRight = negRights.get(owningBucketOrdinal);
        return new InternalGeoBounds(name, top, bottom, posLeft, posRight, negLeft, negRight, wrapLongitude, pipelineAggregators(), metaData());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalGeoBounds(name, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY,
                Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, wrapLongitude, pipelineAggregators(), metaData());
    }
    
    @Override
    public void doClose() {
        Releasables.close(tops, bottoms, posLefts, posRights, negLefts, negRights);
    }

    public static class Factory extends ValuesSourceAggregatorFactory<ValuesSource.GeoPoint> {

        private final boolean wrapLongitude;

        protected Factory(String name, ValuesSourceConfig<ValuesSource.GeoPoint> config, boolean wrapLongitude) {
            super(name, InternalGeoBounds.TYPE.name(), config);
            this.wrapLongitude = wrapLongitude;
        }

        @Override
        protected Aggregator createUnmapped(AggregationContext aggregationContext, Aggregator parent,
                List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) throws IOException {
            return new GeoBoundsAggregator(name, aggregationContext, parent, null, wrapLongitude, pipelineAggregators, metaData);
        }

        @Override
        protected Aggregator doCreateInternal(ValuesSource.GeoPoint valuesSource, AggregationContext aggregationContext, Aggregator parent,
                boolean collectsFromSingleBucket, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData)
                throws IOException {
            return new GeoBoundsAggregator(name, aggregationContext, parent, valuesSource, wrapLongitude, pipelineAggregators, metaData);
        }

    }
}
