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

package org.elasticsearch.search.suggest.completion.context;

import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

import static org.elasticsearch.search.suggest.completion.context.GeoContextMapping.*;

/**
 * Defines the query context for {@link GeoContextMapping}
 */
public class GeoQueryContext implements ToXContent {
    public final CharSequence geoHash;
    public final int precision;
    public final int boost;
    public final int[] neighbours;

    /**
     * Creates a query context for a given geo point with a boost of 1
     * and a precision of {@value GeoContextMapping#DEFAULT_PRECISION}
     */
    public GeoQueryContext(GeoPoint geoPoint) {
        this(geoPoint.geohash());
    }

    /**
     * Creates a query context for a given geo point with a
     * provided boost
     */
    public GeoQueryContext(GeoPoint geoPoint, int boost) {
        this(geoPoint.geohash(), boost);
    }

    /**
     * Creates a query context with a given geo hash with a boost of 1
     * and a precision of {@value GeoContextMapping#DEFAULT_PRECISION}
     */
    public GeoQueryContext(CharSequence geoHash) {
        this(geoHash, 1);
    }

    /**
     * Creates a query context for a given geo hash with a
     * provided boost
     */
    public GeoQueryContext(CharSequence geoHash, int boost) {
        this(geoHash, boost, DEFAULT_PRECISION);
    }

    /**
     * Creates a query context for a geo point with
     * a provided boost and enables generating neighbours
     * at specified precisions
     */
    public GeoQueryContext(GeoPoint geoPoint, int boost, int precision, int... neighbours) {
        this(geoPoint.geohash(), boost, precision, neighbours);
    }

    /**
     * Creates a query context for a geo hash with
     * a provided boost and enables generating neighbours
     * at specified precisions
     */
    public GeoQueryContext(CharSequence geoHash, int boost, int precision, int... neighbours) {
        this.geoHash = geoHash;
        this.boost = boost;
        this.precision = precision;
        this.neighbours = neighbours;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(CONTEXT_VALUE, geoHash);
        builder.field(CONTEXT_BOOST, boost);
        builder.field(CONTEXT_NEIGHBOURS, neighbours);
        builder.endObject();
        return builder;
    }
}
