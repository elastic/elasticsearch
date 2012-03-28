/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Filter;
import org.elasticsearch.common.lucene.docset.GetDocSet;
import org.elasticsearch.index.cache.field.data.FieldDataCache;
import org.elasticsearch.index.mapper.geo.GeoPointFieldData;
import org.elasticsearch.index.mapper.geo.GeoPointFieldDataType;

import java.io.IOException;

/**
 *
 */
public class InMemoryGeoBoundingBoxFilter extends Filter {

    private final Point topLeft;

    private final Point bottomRight;

    private final String fieldName;

    private final FieldDataCache fieldDataCache;

    public InMemoryGeoBoundingBoxFilter(Point topLeft, Point bottomRight, String fieldName, FieldDataCache fieldDataCache) {
        this.topLeft = topLeft;
        this.bottomRight = bottomRight;
        this.fieldName = fieldName;
        this.fieldDataCache = fieldDataCache;
    }

    public Point topLeft() {
        return topLeft;
    }

    public Point bottomRight() {
        return bottomRight;
    }

    public String fieldName() {
        return fieldName;
    }

    @Override
    public DocIdSet getDocIdSet(IndexReader reader) throws IOException {
        final GeoPointFieldData fieldData = (GeoPointFieldData) fieldDataCache.cache(GeoPointFieldDataType.TYPE, reader, fieldName);

        //checks to see if bounding box crosses 180 degrees
        if (topLeft.lon > bottomRight.lon) {
            return new Meridian180GeoBoundingBoxDocSet(reader.maxDoc(), fieldData, topLeft, bottomRight);
        } else {
            return new GeoBoundingBoxDocSet(reader.maxDoc(), fieldData, topLeft, bottomRight);
        }
    }

    @Override
    public String toString() {
        return "GeoBoundingBoxFilter(" + fieldName + ", "  + topLeft + ", " + bottomRight + ")";
    }

    public static class Meridian180GeoBoundingBoxDocSet extends GetDocSet {
        private final GeoPointFieldData fieldData;
        private final Point topLeft;
        private final Point bottomRight;

        public Meridian180GeoBoundingBoxDocSet(int maxDoc, GeoPointFieldData fieldData, Point topLeft, Point bottomRight) {
            super(maxDoc);
            this.fieldData = fieldData;
            this.topLeft = topLeft;
            this.bottomRight = bottomRight;
        }

        @Override
        public boolean isCacheable() {
            // not cacheable for several reasons:
            // 1. It is only relevant when _cache is set to true, and then, we really want to create in mem bitset
            // 2. Its already fast without in mem bitset, since it works with field data
            return false;
        }

        @Override
        public boolean get(int doc) {
            if (!fieldData.hasValue(doc)) {
                return false;
            }

            if (fieldData.multiValued()) {
                double[] lats = fieldData.latValues(doc);
                double[] lons = fieldData.lonValues(doc);
                for (int i = 0; i < lats.length; i++) {
                    double lat = lats[i];
                    double lon = lons[i];
                    if (((topLeft.lon <= lon || bottomRight.lon >= lon)) &&
                            (topLeft.lat >= lat && bottomRight.lat <= lat)) {
                        return true;
                    }
                }
            } else {
                double lat = fieldData.latValue(doc);
                double lon = fieldData.lonValue(doc);

                if (((topLeft.lon <= lon || bottomRight.lon >= lon)) &&
                        (topLeft.lat >= lat && bottomRight.lat <= lat)) {
                    return true;
                }
            }
            return false;
        }
    }

    public static class GeoBoundingBoxDocSet extends GetDocSet {
        private final GeoPointFieldData fieldData;
        private final Point topLeft;
        private final Point bottomRight;

        public GeoBoundingBoxDocSet(int maxDoc, GeoPointFieldData fieldData, Point topLeft, Point bottomRight) {
            super(maxDoc);
            this.fieldData = fieldData;
            this.topLeft = topLeft;
            this.bottomRight = bottomRight;
        }

        @Override
        public boolean isCacheable() {
            // not cacheable for several reasons:
            // 1. It is only relevant when _cache is set to true, and then, we really want to create in mem bitset
            // 2. Its already fast without in mem bitset, since it works with field data
            return false;
        }

        @Override
        public boolean get(int doc) {
            if (!fieldData.hasValue(doc)) {
                return false;
            }

            if (fieldData.multiValued()) {
                double[] lats = fieldData.latValues(doc);
                double[] lons = fieldData.lonValues(doc);
                for (int i = 0; i < lats.length; i++) {
                    if (topLeft.lon <= lons[i] && bottomRight.lon >= lons[i]
                            && topLeft.lat >= lats[i] && bottomRight.lat <= lats[i]) {
                        return true;
                    }
                }
            } else {
                double lat = fieldData.latValue(doc);
                double lon = fieldData.lonValue(doc);

                if (topLeft.lon <= lon && bottomRight.lon >= lon
                        && topLeft.lat >= lat && bottomRight.lat <= lat) {
                    return true;
                }
            }
            return false;
        }
    }
}
