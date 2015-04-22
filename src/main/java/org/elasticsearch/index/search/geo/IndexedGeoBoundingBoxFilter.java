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

import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Filter;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.index.mapper.geo.GeoPointFieldMapper;

/**
 */
public class IndexedGeoBoundingBoxFilter {

    public static Filter create(GeoPoint topLeft, GeoPoint bottomRight, GeoPointFieldMapper fieldMapper) {
        if (!fieldMapper.isEnableLatLon()) {
            throw new ElasticsearchIllegalArgumentException("lat/lon is not enabled (indexed) for field [" + fieldMapper.name() + "], can't use indexed filter on it");
        }
        //checks to see if bounding box crosses 180 degrees
        if (topLeft.lon() > bottomRight.lon()) {
            return westGeoBoundingBoxFilter(topLeft, bottomRight, fieldMapper);
        } else {
            return eastGeoBoundingBoxFilter(topLeft, bottomRight, fieldMapper);
        }
    }

    private static Filter westGeoBoundingBoxFilter(GeoPoint topLeft, GeoPoint bottomRight, GeoPointFieldMapper fieldMapper) {
        BooleanQuery filter = new BooleanQuery();
        filter.setMinimumNumberShouldMatch(1);
        filter.add(fieldMapper.lonMapper().rangeFilter(null, bottomRight.lon(), true, true), Occur.SHOULD);
        filter.add(fieldMapper.lonMapper().rangeFilter(topLeft.lon(), null, true, true), Occur.SHOULD);
        filter.add(fieldMapper.latMapper().rangeFilter(bottomRight.lat(), topLeft.lat(), true, true), Occur.MUST);
        return Queries.wrap(filter);
    }

    private static Filter eastGeoBoundingBoxFilter(GeoPoint topLeft, GeoPoint bottomRight, GeoPointFieldMapper fieldMapper) {
        BooleanQuery filter = new BooleanQuery();
        filter.add(fieldMapper.lonMapper().rangeFilter(topLeft.lon(), bottomRight.lon(), true, true), Occur.MUST);
        filter.add(fieldMapper.latMapper().rangeFilter(bottomRight.lat(), topLeft.lat(), true, true), Occur.MUST);
        return Queries.wrap(filter);
    }
}
