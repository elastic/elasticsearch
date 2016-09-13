/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene.spatial.geopoint.search;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.spatial.geopoint.document.GeoPointField.TermEncoding;

/** Implements a point distance range query on a GeoPoint field. This is based on
 * {@code org.apache.lucene.spatial.geopoint.search.GeoPointDistanceQuery} and is implemented using a
 * {@code org.apache.lucene.search.BooleanClause.MUST_NOT} clause to exclude any points that fall within
 * minRadiusMeters from the provided point.
 * <p>
 * NOTE: this query does not correctly support multi-value docs (see: https://issues.apache.org/jira/browse/LUCENE-7126)
 * <br>
 * TODO: remove this per ISSUE #17658
 **/
public final class XGeoPointDistanceRangeQuery extends GeoPointDistanceQuery {
    /** minimum distance range (in meters) from lat, lon center location, maximum is inherited */
    protected final double minRadiusMeters;

    /**
     * Constructs a query for all {@link org.apache.lucene.spatial.geopoint.document.GeoPointField} types within a minimum / maximum
     * distance (in meters) range from a given point
     */
    public XGeoPointDistanceRangeQuery(final String field, final double centerLat, final double centerLon,
                                      final double minRadiusMeters, final double maxRadiusMeters) {
        this(field, TermEncoding.PREFIX, centerLat, centerLon, minRadiusMeters, maxRadiusMeters);
    }

    /**
     * Constructs a query for all {@link org.apache.lucene.spatial.geopoint.document.GeoPointField} types within a minimum / maximum
     * distance (in meters) range from a given point. Accepts an optional
     * {@link org.apache.lucene.spatial.geopoint.document.GeoPointField.TermEncoding}
     */
    public XGeoPointDistanceRangeQuery(final String field, final TermEncoding termEncoding, final double centerLat, final double centerLon,
                                      final double minRadiusMeters, final double maxRadius) {
        super(field, termEncoding, centerLat, centerLon, maxRadius);
        this.minRadiusMeters = minRadiusMeters;
    }

    @Override
    public Query rewrite(IndexReader reader) {
        Query q = super.rewrite(reader);
        if (minRadiusMeters == 0.0) {
            return q;
        }

        // add an exclusion query
        BooleanQuery.Builder bqb = new BooleanQuery.Builder();

        // create a new exclusion query
        GeoPointDistanceQuery exclude = new GeoPointDistanceQuery(field, termEncoding, centerLat, centerLon, minRadiusMeters);
        // full map search
//    if (radiusMeters >= GeoProjectionUtils.SEMIMINOR_AXIS) {
//      bqb.add(new BooleanClause(new GeoPointInBBoxQuery(this.field, -180.0, -90.0, 180.0, 90.0), BooleanClause.Occur.MUST));
//    } else {
        bqb.add(new BooleanClause(q, BooleanClause.Occur.MUST));
//    }
        bqb.add(new BooleanClause(exclude, BooleanClause.Occur.MUST_NOT));

        return bqb.build();
    }

    @Override
    public String toString(String field) {
        final StringBuilder sb = new StringBuilder();
        sb.append(getClass().getSimpleName());
        sb.append(':');
        if (!this.field.equals(field)) {
            sb.append(" field=");
            sb.append(this.field);
            sb.append(':');
        }
        return sb.append( " Center: [")
            .append(centerLat)
            .append(',')
            .append(centerLon)
            .append(']')
            .append(" From Distance: ")
            .append(minRadiusMeters)
            .append(" m")
            .append(" To Distance: ")
            .append(radiusMeters)
            .append(" m")
            .append(" Lower Left: [")
            .append(minLat)
            .append(',')
            .append(minLon)
            .append(']')
            .append(" Upper Right: [")
            .append(maxLat)
            .append(',')
            .append(maxLon)
            .append("]")
            .toString();
    }

    /** getter method for minimum distance */
    public double getMinRadiusMeters() {
        return this.minRadiusMeters;
    }

    /** getter method for maximum distance */
    public double getMaxRadiusMeters() {
        return this.radiusMeters;
    }
}
