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

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.ValuesSourceAggregationBuilder;

import java.io.IOException;

/**
 * Builder for the {@link GeoBounds} aggregation.
 */
public class GeoBoundsBuilder extends ValuesSourceAggregationBuilder<GeoBoundsBuilder> {

    private Boolean wrapLongitude;

    /**
     * Sole constructor.
     */
    public GeoBoundsBuilder(String name) {
        super(name, InternalGeoBounds.TYPE.name());
    }

    /**
     * Set whether to wrap longitudes. Defaults to true.
     */
    public GeoBoundsBuilder wrapLongitude(boolean wrapLongitude) {
        this.wrapLongitude = wrapLongitude;
        return this;
    }

    @Override
    protected XContentBuilder doInternalXContent(XContentBuilder builder, Params params) throws IOException {
        if (wrapLongitude != null) {
            builder.field("wrap_longitude", wrapLongitude);
        }
        return builder;
    }

}
