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
import org.elasticsearch.common.geo.GeoExtent;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ObjectArray;
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
    private ObjectArray<GeoExtent> extents;
    // size in bytes of an extent object for accounting
    private static int EXTENT_WEIGHT = 6 * Double.BYTES;

    GeoBoundsAggregator(String name, SearchContext aggregationContext, Aggregator parent,
            ValuesSource.GeoPoint valuesSource, boolean wrapLongitude, List<PipelineAggregator> pipelineAggregators,
            Map<String, Object> metaData) throws IOException {
        super(name, aggregationContext, parent, pipelineAggregators, metaData);
        this.valuesSource = valuesSource;
        this.wrapLongitude = wrapLongitude;
        if (valuesSource != null) {
            final BigArrays bigArrays = context.bigArrays();
            extents = bigArrays.newObjectArray(1);
            extents.set(0, createGeoExtentAndAccount());
        } else {
            extents = null;
        }
    }

    private GeoExtent createGeoExtentAndAccount() {
        addRequestCircuitBreakerBytes(EXTENT_WEIGHT);
        return new GeoExtent();
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
                if (bucket >= extents.size()) {
                    long from = extents.size();
                    extents = bigArrays.grow(extents, bucket + 1);
                    for (long i = from; i < extents.size(); i++) {
                        extents.set(i, createGeoExtentAndAccount());
                    }
                }

                if (values.advanceExact(doc)) {
                    final int valuesCount = values.docValueCount();
                    for (int i = 0; i < valuesCount; ++i) {
                        GeoPoint value = values.nextValue();
                        GeoExtent extent = extents.get(bucket);
                        extent.addPoint(value.lat(), value.lon());
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
        GeoExtent extent = extents.get(owningBucketOrdinal);
        return new InternalGeoBounds(name, extent, wrapLongitude, pipelineAggregators(), metaData());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalGeoBounds(name, new GeoExtent(), wrapLongitude, pipelineAggregators(), metaData());
    }

    @Override
    public void doClose() {
        if (extents != null) {
            addRequestCircuitBreakerBytes(-1 * EXTENT_WEIGHT * extents.size());
            Releasables.close(extents);
        }
    }
}
