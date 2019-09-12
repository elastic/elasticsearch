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

import org.apache.lucene.geo.GeoEncodingUtils;
import org.elasticsearch.Version;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Serialization and merge logic for {@link GeoCentroidAggregator}.
 */
public class InternalGeoCentroid extends InternalAggregation implements GeoCentroid {
    private final GeoPoint centroid;
    private final long count;

    public static long encodeLatLon(double lat, double lon) {
        return (Integer.toUnsignedLong(GeoEncodingUtils.encodeLatitude(lat)) << 32) |
                    Integer.toUnsignedLong(GeoEncodingUtils.encodeLongitude(lon));
    }

    public static double decodeLatitude(long encodedLatLon) {
        return GeoEncodingUtils.decodeLatitude((int) (encodedLatLon >>> 32));
    }

    public static double decodeLongitude(long encodedLatLon) {
        return GeoEncodingUtils.decodeLongitude((int) (encodedLatLon & 0xFFFFFFFFL));
    }

    InternalGeoCentroid(String name, GeoPoint centroid, long count, List<PipelineAggregator>
            pipelineAggregators, Map<String, Object> metaData) {
        super(name, pipelineAggregators, metaData);
        assert (centroid == null) == (count == 0);
        this.centroid = centroid;
        assert count >= 0;
        this.count = count;
    }

    /**
     * Read from a stream.
     */
    public InternalGeoCentroid(StreamInput in) throws IOException {
        super(in);
        count = in.readVLong();
        if (in.readBoolean()) {
            if (in.getVersion().onOrAfter(Version.V_7_2_0)) {
                centroid = new GeoPoint(in.readDouble(), in.readDouble());
            } else {
                final long hash = in.readLong();
                centroid = new GeoPoint(decodeLatitude(hash), decodeLongitude(hash));
            }

        } else {
            centroid = null;
        }
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeVLong(count);
        if (centroid != null) {
            out.writeBoolean(true);
            if (out.getVersion().onOrAfter(Version.V_7_2_0)) {
                out.writeDouble(centroid.lat());
                out.writeDouble(centroid.lon());
            } else {
                out.writeLong(encodeLatLon(centroid.lat(), centroid.lon()));
            }
        } else {
            out.writeBoolean(false);
        }
    }

    @Override
    public String getWriteableName() {
        return GeoCentroidAggregationBuilder.NAME;
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
                case "count":
                    return count;
                default:
                    throw new IllegalArgumentException("Found unknown path element [" + coordinate + "] in [" + getName() + "]");
            }
        } else {
            throw new IllegalArgumentException("path not supported for [" + getName() + "]: " + path);
        }
    }

    static class Fields {
        static final ParseField CENTROID = new ParseField("location");
        static final ParseField COUNT = new ParseField("count");
        static final ParseField CENTROID_LAT = new ParseField("lat");
        static final ParseField CENTROID_LON = new ParseField("lon");
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        if (centroid != null) {
            builder.startObject(Fields.CENTROID.getPreferredName());
            {
                builder.field(Fields.CENTROID_LAT.getPreferredName(), centroid.lat());
                builder.field(Fields.CENTROID_LON.getPreferredName(), centroid.lon());
            }
            builder.endObject();
        }
        builder.field(Fields.COUNT.getPreferredName(), count);
        return builder;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;
        InternalGeoCentroid that = (InternalGeoCentroid) obj;
        return count == that.count &&
                Objects.equals(centroid, that.centroid);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), centroid, count);
    }

    @Override
    public String toString() {
        return "InternalGeoCentroid{" +
                "centroid=" + centroid +
                ", count=" + count +
                '}';
    }
}
