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

package org.elasticsearch.search.aggregations.metrics.geocentroid;

import org.apache.lucene.spatial.geopoint.document.GeoPointField;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.AggregationStreams;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.InternalMetricsAggregation;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Serialization and merge logic for {@link org.elasticsearch.search.aggregations.metrics.geocentroid.GeoCentroidAggregator}
 */
public class InternalGeoCentroid extends InternalMetricsAggregation implements GeoCentroid {

    public final static Type TYPE = new Type("geo_centroid");
    public final static AggregationStreams.Stream STREAM = new AggregationStreams.Stream() {
        @Override
        public InternalGeoCentroid readResult(StreamInput in) throws IOException {
            InternalGeoCentroid result = new InternalGeoCentroid();
            result.readFrom(in);
            return result;
        }
    };

    public static void registerStreams() {
        AggregationStreams.registerStream(STREAM, TYPE.stream());
    }

    protected GeoPoint centroid;
    protected long count;

    protected InternalGeoCentroid() {
    }

    public InternalGeoCentroid(String name, GeoPoint centroid, long count, List<PipelineAggregator>
            pipelineAggregators, Map<String, Object> metaData) {
        super(name, pipelineAggregators, metaData);
        assert (centroid == null) == (count == 0);
        this.centroid = centroid;
        assert count >= 0;
        this.count = count;
    }

    @Override
    public GeoPoint centroid() {
        return centroid;
    }

    @Override
    public long count() {
        return count;
    }

    @Override
    public Type type() {
        return TYPE;
    }

    @Override
    public InternalGeoCentroid doReduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        double lonSum = Double.NaN;
        double latSum = Double.NaN;
        int totalCount = 0;
        for (InternalAggregation aggregation : aggregations) {
            InternalGeoCentroid centroidAgg = (InternalGeoCentroid) aggregation;
            if (centroidAgg.count > 0) {
                totalCount += centroidAgg.count;
                if (Double.isNaN(lonSum)) {
                    lonSum = centroidAgg.count * centroidAgg.centroid.getLon();
                    latSum = centroidAgg.count * centroidAgg.centroid.getLat();
                } else {
                    lonSum += (centroidAgg.count * centroidAgg.centroid.getLon());
                    latSum += (centroidAgg.count * centroidAgg.centroid.getLat());
                }
            }
        }
        final GeoPoint result = (Double.isNaN(lonSum)) ? null : new GeoPoint(latSum/totalCount, lonSum/totalCount);
        return new InternalGeoCentroid(name, result, totalCount, pipelineAggregators(), getMetaData());
    }

    @Override
    public Object getProperty(List<String> path) {
        if (path.isEmpty()) {
            return this;
        } else if (path.size() == 1) {
            String coordinate = path.get(0);
            switch (coordinate) {
                case "value":
                    return centroid;
                case "lat":
                    return centroid.lat();
                case "lon":
                    return centroid.lon();
                default:
                    throw new IllegalArgumentException("Found unknown path element [" + coordinate + "] in [" + getName() + "]");
            }
        } else {
            throw new IllegalArgumentException("path not supported for [" + getName() + "]: " + path);
        }
    }

    @Override
    protected void doReadFrom(StreamInput in) throws IOException {
        count = in.readVLong();
        if (in.readBoolean()) {
            final long hash = in.readLong();
            centroid = new GeoPoint(GeoPointField.decodeLatitude(hash), GeoPointField.decodeLongitude(hash));
        } else {
            centroid = null;
        }
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeVLong(count);
        if (centroid != null) {
            out.writeBoolean(true);
            // should we just write lat and lon separately?
            out.writeLong(GeoPointField.encodeLatLon(centroid.lat(), centroid.lon()));
        } else {
            out.writeBoolean(false);
        }
    }

    static class Fields {
        public static final String CENTROID = "location";
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        if (centroid != null) {
            builder.startObject(Fields.CENTROID).field("lat", centroid.lat()).field("lon", centroid.lon()).endObject();
        }
        return builder;
    }
}
