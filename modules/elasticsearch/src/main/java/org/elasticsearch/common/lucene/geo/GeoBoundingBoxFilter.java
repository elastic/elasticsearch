/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

package org.elasticsearch.common.lucene.geo;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Filter;
import org.elasticsearch.common.lucene.docset.GetDocSet;
import org.elasticsearch.index.cache.field.data.FieldDataCache;
import org.elasticsearch.index.field.data.FieldData;
import org.elasticsearch.index.field.data.NumericFieldData;

import java.io.IOException;

/**
 * @author kimchy (shay.banon)
 */
public class GeoBoundingBoxFilter extends Filter {

    private final Point topLeft;

    private final Point bottomRight;

    private final String latFieldName;

    private final String lonFieldName;

    private final FieldData.Type fieldDataType;

    private final FieldDataCache fieldDataCache;

    public GeoBoundingBoxFilter(Point topLeft, Point bottomRight, String latFieldName, String lonFieldName, FieldData.Type fieldDataType, FieldDataCache fieldDataCache) {
        this.topLeft = topLeft;
        this.bottomRight = bottomRight;
        this.latFieldName = latFieldName;
        this.lonFieldName = lonFieldName;
        this.fieldDataType = fieldDataType;
        this.fieldDataCache = fieldDataCache;
    }

    public Point topLeft() {
        return topLeft;
    }

    public Point bottomRight() {
        return bottomRight;
    }

    public String latFieldName() {
        return latFieldName;
    }

    public String lonFieldName() {
        return lonFieldName;
    }

    @Override public DocIdSet getDocIdSet(IndexReader reader) throws IOException {
        final NumericFieldData latFieldData = (NumericFieldData) fieldDataCache.cache(fieldDataType, reader, latFieldName);
        final NumericFieldData lonFieldData = (NumericFieldData) fieldDataCache.cache(fieldDataType, reader, lonFieldName);

        //checks to see if bounding box crosses 180 degrees
        if (topLeft.lon > bottomRight.lon) {
            return new GetDocSet(reader.maxDoc()) {
                @Override public boolean get(int doc) throws IOException {
                    if (!latFieldData.hasValue(doc) || !lonFieldData.hasValue(doc)) {
                        return false;
                    }

                    if (latFieldData.multiValued()) {
                        double[] lats = latFieldData.doubleValues(doc);
                        double[] lons = latFieldData.doubleValues(doc);
                        for (int i = 0; i < lats.length; i++) {
                            double lat = lats[i];
                            double lon = lons[i];
                            if (lon < 0) {
                                if (-180.0 <= lon && bottomRight.lon >= lon
                                        && topLeft.lat >= lat && bottomRight.lat <= lat) {
                                    return true;
                                }
                            } else {
                                if (topLeft.lon <= lon && 180 >= lon
                                        && topLeft.lat >= lat && bottomRight.lat <= lat) {
                                    return true;
                                }
                            }
                        }
                    } else {
                        double lat = latFieldData.doubleValue(doc);
                        double lon = lonFieldData.doubleValue(doc);
                        if (lon < 0) {
                            if (-180.0 <= lon && bottomRight.lon >= lon
                                    && topLeft.lat >= lat && bottomRight.lat <= lat) {
                                return true;
                            }
                        } else {
                            if (topLeft.lon <= lon && 180 >= lon
                                    && topLeft.lat >= lat && bottomRight.lat <= lat) {
                                return true;
                            }
                        }
                    }
                    return false;
                }
            };
        } else {
            return new GetDocSet(reader.maxDoc()) {
                @Override public boolean get(int doc) throws IOException {
                    if (!latFieldData.hasValue(doc) || !lonFieldData.hasValue(doc)) {
                        return false;
                    }

                    if (latFieldData.multiValued()) {
                        double[] lats = latFieldData.doubleValues(doc);
                        double[] lons = latFieldData.doubleValues(doc);
                        for (int i = 0; i < lats.length; i++) {
                            if (topLeft.lon <= lons[i] && bottomRight.lon >= lons[i]
                                    && topLeft.lat >= lats[i] && bottomRight.lat <= lats[i]) {
                                return true;
                            }
                        }
                    } else {
                        double lat = latFieldData.doubleValue(doc);
                        double lon = lonFieldData.doubleValue(doc);

                        if (topLeft.lon <= lon && bottomRight.lon >= lon
                                && topLeft.lat >= lat && bottomRight.lat <= lat) {
                            return true;
                        }
                    }
                    return false;
                }
            };
        }
    }

    public static class Point {
        public double lat;
        public double lon;
    }
}
