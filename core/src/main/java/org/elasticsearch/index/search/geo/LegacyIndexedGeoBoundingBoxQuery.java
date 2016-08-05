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

package org.elasticsearch.index.search.geo;

import org.apache.lucene.geo.Rectangle;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.index.mapper.BaseGeoPointFieldMapper.LegacyGeoPointFieldType;
import org.elasticsearch.index.query.QueryShardContext;

/**
 * Bounding Box filter used for indexes created before 2.2
 *
 * @deprecated this class is deprecated in favor of lucene's GeoPointField
 */
@Deprecated
public class LegacyIndexedGeoBoundingBoxQuery {
    public static Query create(final Rectangle bbox, final LegacyGeoPointFieldType fieldType, QueryShardContext context) {
        return create(bbox.minLat, bbox.minLon, bbox.maxLat, bbox.maxLon, fieldType, context);
    }

    public static Query create(final GeoPoint topLeft, final GeoPoint bottomRight, final LegacyGeoPointFieldType fieldType,
                               QueryShardContext context) {
        return create(bottomRight.getLat(), topLeft.getLon(), topLeft.getLat(), bottomRight.getLon(), fieldType, context);
    }

    private static Query create(double minLat, double minLon, double maxLat, double maxLon,
                                LegacyGeoPointFieldType fieldType, QueryShardContext context) {
        if (!fieldType.isLatLonEnabled()) {
            throw new IllegalArgumentException("lat/lon is not enabled (indexed) for field [" + fieldType.name()
                + "], can't use indexed filter on it");
        }
        //checks to see if bounding box crosses 180 degrees
        if (minLon > maxLon) {
            return westGeoBoundingBoxFilter(minLat, minLon, maxLat, maxLon, fieldType, context);
        }
        return eastGeoBoundingBoxFilter(minLat, minLon, maxLat, maxLon, fieldType, context);
    }

    private static Query westGeoBoundingBoxFilter(double minLat, double minLon, double maxLat, double maxLon,
                                                  LegacyGeoPointFieldType fieldType, QueryShardContext context) {
        BooleanQuery.Builder filter = new BooleanQuery.Builder();
        filter.setMinimumNumberShouldMatch(1);
        filter.add(fieldType.lonFieldType().rangeQuery(null, maxLon, true, true, context), Occur.SHOULD);
        filter.add(fieldType.lonFieldType().rangeQuery(minLon, null, true, true, context), Occur.SHOULD);
        filter.add(fieldType.latFieldType().rangeQuery(minLat, maxLat, true, true, context), Occur.MUST);
        return new ConstantScoreQuery(filter.build());
    }

    private static Query eastGeoBoundingBoxFilter(double minLat, double minLon, double maxLat, double maxLon,
                                                  LegacyGeoPointFieldType fieldType, QueryShardContext context) {
        BooleanQuery.Builder filter = new BooleanQuery.Builder();
        filter.add(fieldType.lonFieldType().rangeQuery(minLon, maxLon, true, true, context), Occur.MUST);
        filter.add(fieldType.latFieldType().rangeQuery(minLat, maxLat, true, true, context), Occur.MUST);
        return new ConstantScoreQuery(filter.build());
    }
}
