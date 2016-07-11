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

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories.Builder;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;

import java.io.IOException;
import java.util.Objects;

public class GeoBoundsAggregationBuilder extends ValuesSourceAggregationBuilder<ValuesSource.GeoPoint, GeoBoundsAggregationBuilder> {
    public static final String NAME = InternalGeoBounds.TYPE.name();
    public static final ParseField AGGREGATION_NAME_FIED = new ParseField(NAME);

    private boolean wrapLongitude = true;

    public GeoBoundsAggregationBuilder(String name) {
        super(name, InternalGeoBounds.TYPE, ValuesSourceType.GEOPOINT, ValueType.GEOPOINT);
    }

    /**
     * Read from a stream.
     */
    public GeoBoundsAggregationBuilder(StreamInput in) throws IOException {
        super(in, InternalGeoBounds.TYPE, ValuesSourceType.GEOPOINT, ValueType.GEOPOINT);
        wrapLongitude = in.readBoolean();
    }

    @Override
    protected void innerWriteTo(StreamOutput out) throws IOException {
        out.writeBoolean(wrapLongitude);
    }

    /**
     * Set whether to wrap longitudes. Defaults to true.
     */
    public GeoBoundsAggregationBuilder wrapLongitude(boolean wrapLongitude) {
        this.wrapLongitude = wrapLongitude;
        return this;
    }

    /**
     * Get whether to wrap longitudes.
     */
    public boolean wrapLongitude() {
        return wrapLongitude;
    }

    @Override
    protected GeoBoundsAggregatorFactory innerBuild(AggregationContext context, ValuesSourceConfig<ValuesSource.GeoPoint> config,
            AggregatorFactory<?> parent, Builder subFactoriesBuilder) throws IOException {
        return new GeoBoundsAggregatorFactory(name, type, config, wrapLongitude, context, parent, subFactoriesBuilder, metaData);
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(GeoBoundsAggregator.WRAP_LONGITUDE_FIELD.getPreferredName(), wrapLongitude);
        return builder;
    }

    @Override
    protected int innerHashCode() {
        return Objects.hash(wrapLongitude);
    }

    @Override
    protected boolean innerEquals(Object obj) {
        GeoBoundsAggregationBuilder other = (GeoBoundsAggregationBuilder) obj;
        return Objects.equals(wrapLongitude, other.wrapLongitude);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
}
