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

package org.elasticsearch.search.aggregations.metrics;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.DoubleArray;
import org.elasticsearch.index.fielddata.MultiGeoPointValues;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;

final class GeoBoundsAggregator extends MetricsAggregator {

    static final ParseField WRAP_LONGITUDE_FIELD = new ParseField("wrap_longitude");

    private final ValuesSource.GeoPoint valuesSource;
    private final boolean wrapLongitude;
    GeoExtent extent;

    GeoBoundsAggregator(String name, SearchContext aggregationContext, Aggregator parent,
                        ValuesSource.GeoPoint valuesSource, boolean wrapLongitude, List<PipelineAggregator> pipelineAggregators,
                        Map<String, Object> metaData) throws IOException {
        super(name, aggregationContext, parent, pipelineAggregators, metaData);
        this.valuesSource = valuesSource;
        this.wrapLongitude = wrapLongitude;
        if (valuesSource != null) {
            final BigArrays bigArrays = context.bigArrays();
            extent = new GeoExtent(bigArrays);
        }
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx,
                                                LeafBucketCollector sub) {
        if (valuesSource == null) {
            return LeafBucketCollector.NO_OP_COLLECTOR;
        }
        final MultiGeoPointValues values = valuesSource.geoPointValues(ctx);
        return new LeafBucketCollectorBase(sub, values) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                if (bucket >= extent.size()) {
                    extent.grow(bucket + 1);
                }

                if (values.advanceExact(doc)) {
                    final int valuesCount = values.docValueCount();

                    for (int i = 0; i < valuesCount; ++i) {
                        GeoPoint value = values.nextValue();
                        extent.addPoint(bucket, value.lat(), value.lon());
                    }
                }
            }
        };
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) {
        if (valuesSource == null) {
            return buildEmptyAggregation();
        }
        double top = extent.tops.get(owningBucketOrdinal);
        double bottom = extent.bottoms.get(owningBucketOrdinal);
        double posLeft = extent.posLefts.get(owningBucketOrdinal);
        double posRight = extent.posRights.get(owningBucketOrdinal);
        double negLeft = extent.negLefts.get(owningBucketOrdinal);
        double negRight = extent.negRights.get(owningBucketOrdinal);
        return new InternalGeoBounds(name, top, bottom, posLeft, posRight, negLeft, negRight, wrapLongitude,
            pipelineAggregators(), metaData());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalGeoBounds(name, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY,
            Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, wrapLongitude,
            pipelineAggregators(), metaData());
    }

    @Override
    public void doClose() {
        Releasables.close(extent);
    }

    private static class GeoExtent implements Releasable {

        DoubleArray tops;
        DoubleArray bottoms;
        DoubleArray posLefts;
        DoubleArray posRights;
        DoubleArray negLefts;
        DoubleArray negRights;
        BigArrays bigArrays;

        GeoExtent(BigArrays bigArrays) {
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
            this.bigArrays = bigArrays;
        }

        /**
         * grow the size of the arrays
         */
        public void grow(long bucket) {
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

        /**
         * Size of the arrays
         */
        public long size() {
            return tops.size();
        }

        /**
         * Adds a point to the Extent at given position
         */
        public void addPoint(long bucket, double lat, double lon) {
            this.tops.set(bucket,  Math.max(this.tops.get(bucket), lat));
            this.bottoms.set(bucket, Math.min(this.bottoms.get(bucket), lat));
            if (lon < 0) {
                this.negLefts.set(bucket, Math.min(this.negLefts.get(bucket), lon));
                this.negRights.set(bucket, Math.max(this.negRights.get(bucket), lon));
            } else {
                this.posLefts.set(bucket, Math.min(this.posLefts.get(bucket), lon));
                this.posRights.set(bucket, Math.max(this.posRights.get(bucket), lon));
            }
        }

        @Override
        public void close() {
            Releasables.close(tops, bottoms, posLefts, posRights, negLefts, negRights);
        }
    }
}
