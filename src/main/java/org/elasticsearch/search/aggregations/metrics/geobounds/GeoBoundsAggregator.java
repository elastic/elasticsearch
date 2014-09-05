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

import org.apache.lucene.index.AtomicReaderContext;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.DoubleArray;
import org.elasticsearch.index.fielddata.MultiGeoPointValues;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.MetricsAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;

import java.io.IOException;

public final class GeoBoundsAggregator extends MetricsAggregator {

    private final ValuesSource.GeoPoint valuesSource;
    private final boolean wrapLongitude;
    private DoubleArray tops;
    private DoubleArray bottoms;
    private DoubleArray posLefts;
    private DoubleArray posRights;
    private DoubleArray negLefts;
    private DoubleArray negRights;
    private MultiGeoPointValues values;

    protected GeoBoundsAggregator(String name, long estimatedBucketsCount,
            AggregationContext aggregationContext, Aggregator parent, ValuesSource.GeoPoint valuesSource, boolean wrapLongitude) {
        super(name, estimatedBucketsCount, aggregationContext, parent);
        this.valuesSource = valuesSource;
        this.wrapLongitude = wrapLongitude;
        if (valuesSource != null) {
            final long initialSize = estimatedBucketsCount < 2 ? 1 : estimatedBucketsCount;
            tops = bigArrays.newDoubleArray(initialSize, false);
            tops.fill(0, tops.size(), Double.NEGATIVE_INFINITY);
            bottoms = bigArrays.newDoubleArray(initialSize, false);
            bottoms.fill(0, bottoms.size(), Double.POSITIVE_INFINITY);
            posLefts = bigArrays.newDoubleArray(initialSize, false);
            posLefts.fill(0, posLefts.size(), Double.POSITIVE_INFINITY);
            posRights = bigArrays.newDoubleArray(initialSize, false);
            posRights.fill(0, posRights.size(), Double.NEGATIVE_INFINITY);
            negLefts = bigArrays.newDoubleArray(initialSize, false);
            negLefts.fill(0, negLefts.size(), Double.POSITIVE_INFINITY);
            negRights = bigArrays.newDoubleArray(initialSize, false);
            negRights.fill(0, negRights.size(), Double.NEGATIVE_INFINITY);
        }
    }

    @Override
    public boolean shouldCollect() {
        return valuesSource != null;
    }

    @Override
    public void setNextReader(AtomicReaderContext reader) {
        this.values = this.valuesSource.geoPointValues();
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
        return new InternalGeoBounds(name, top, bottom, posLeft, posRight, negLeft, negRight, wrapLongitude);
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalGeoBounds(name, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY,
                Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, wrapLongitude);
    }

    @Override
    public void collect(int docId, long owningBucketOrdinal) throws IOException {
        if (owningBucketOrdinal >= tops.size()) {
            long from = tops.size();
            tops = bigArrays.grow(tops, owningBucketOrdinal + 1);
            tops.fill(from, tops.size(), Double.NEGATIVE_INFINITY);
            bottoms = bigArrays.resize(bottoms, tops.size());
            bottoms.fill(from, bottoms.size(), Double.NEGATIVE_INFINITY);
            posLefts = bigArrays.resize(posLefts, tops.size());
            posLefts.fill(from, posLefts.size(), Double.NEGATIVE_INFINITY);
            posRights = bigArrays.resize(posRights, tops.size());
            posRights.fill(from, posRights.size(), Double.NEGATIVE_INFINITY);
            negLefts = bigArrays.resize(negLefts, tops.size());
            negLefts.fill(from, negLefts.size(), Double.NEGATIVE_INFINITY);
            negRights = bigArrays.resize(negRights, tops.size());
            negRights.fill(from, negRights.size(), Double.NEGATIVE_INFINITY);
        }

        values.setDocument(docId);
        final int valuesCount = values.count();

        for (int i = 0; i < valuesCount; ++i) {
            GeoPoint value = values.valueAt(i);
            double top = tops.get(owningBucketOrdinal);
            if (value.lat() > top) {
                top = value.lat();
            }
            double bottom = bottoms.get(owningBucketOrdinal);
            if (value.lat() < bottom) {
                bottom = value.lat();
            }
            double posLeft = posLefts.get(owningBucketOrdinal);
            if (value.lon() > 0 && value.lon() < posLeft) {
                posLeft = value.lon();
            }
            double posRight = posRights.get(owningBucketOrdinal);
            if (value.lon() > 0 && value.lon() > posRight) {
                posRight = value.lon();
            }
            double negLeft = negLefts.get(owningBucketOrdinal);
            if (value.lon() < 0 && value.lon() < negLeft) {
                negLeft = value.lon();
            }
            double negRight = negRights.get(owningBucketOrdinal);
            if (value.lon() < 0 && value.lon() > negRight) {
                negRight = value.lon();
            }
            tops.set(owningBucketOrdinal, top);
            bottoms.set(owningBucketOrdinal, bottom);
            posLefts.set(owningBucketOrdinal, posLeft);
            posRights.set(owningBucketOrdinal, posRight);
            negLefts.set(owningBucketOrdinal, negLeft);
            negRights.set(owningBucketOrdinal, negRight);
        }
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
        protected Aggregator createUnmapped(AggregationContext aggregationContext, Aggregator parent) {
            return new GeoBoundsAggregator(name, 0, aggregationContext, parent, null, wrapLongitude);
        }

        @Override
        protected Aggregator create(ValuesSource.GeoPoint valuesSource, long expectedBucketsCount, AggregationContext aggregationContext,
                Aggregator parent) {
            return new GeoBoundsAggregator(name, expectedBucketsCount, aggregationContext, parent, valuesSource, wrapLongitude);
        }

    }
}
