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

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.Bits;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.lucene.docset.MatchDocIdSet;
import org.elasticsearch.index.fielddata.GeoPointValues;
import org.elasticsearch.index.fielddata.IndexGeoPointFieldData;
import org.elasticsearch.index.mapper.geo.GeoPoint;

import java.io.IOException;

/**
 *
 */
public class InMemoryGeoBoundingBoxFilter extends Filter {

    private final Point topLeft;
    private final Point bottomRight;

    private final IndexGeoPointFieldData indexFieldData;

    public InMemoryGeoBoundingBoxFilter(Point topLeft, Point bottomRight, IndexGeoPointFieldData indexFieldData) {
        this.topLeft = topLeft;
        this.bottomRight = bottomRight;
        this.indexFieldData = indexFieldData;
    }

    public Point topLeft() {
        return topLeft;
    }

    public Point bottomRight() {
        return bottomRight;
    }

    public String fieldName() {
        return indexFieldData.getFieldNames().indexName();
    }

    @Override
    public DocIdSet getDocIdSet(AtomicReaderContext context, Bits acceptedDocs) throws IOException {
        final GeoPointValues values = indexFieldData.load(context).getGeoPointValues();

        //checks to see if bounding box crosses 180 degrees
        if (topLeft.lon > bottomRight.lon) {
            return new Meridian180GeoBoundingBoxDocSet(context.reader().maxDoc(), acceptedDocs, values, topLeft, bottomRight);
        } else {
            return new GeoBoundingBoxDocSet(context.reader().maxDoc(), acceptedDocs, values, topLeft, bottomRight);
        }
    }

    @Override
    public String toString() {
        return "GeoBoundingBoxFilter(" + indexFieldData.getFieldNames().indexName() + ", " + topLeft + ", " + bottomRight + ")";
    }

    public static class Meridian180GeoBoundingBoxDocSet extends MatchDocIdSet {
        private final GeoPointValues values;
        private final Point topLeft;
        private final Point bottomRight;

        public Meridian180GeoBoundingBoxDocSet(int maxDoc, @Nullable Bits acceptDocs, GeoPointValues values, Point topLeft, Point bottomRight) {
            super(maxDoc, acceptDocs);
            this.values = values;
            this.topLeft = topLeft;
            this.bottomRight = bottomRight;
        }

        @Override
        public boolean isCacheable() {
            return true;
        }

        @Override
        protected boolean matchDoc(int doc) {
            if (!values.hasValue(doc)) {
                return false;
            }

            if (values.isMultiValued()) {
                GeoPointValues.Iter iter = values.getIter(doc);
                while (iter.hasNext()) {
                    GeoPoint point = iter.next();
                    if (((topLeft.lon <= point.lon() || bottomRight.lon >= point.lon())) &&
                            (topLeft.lat >= point.lat() && bottomRight.lat <= point.lat())) {
                        return true;
                    }
                }
            } else {
                GeoPoint point = values.getValue(doc);

                if (((topLeft.lon <= point.lon() || bottomRight.lon >= point.lon())) &&
                        (topLeft.lat >= point.lat() && bottomRight.lat <= point.lat())) {
                    return true;
                }
            }
            return false;
        }
    }

    public static class GeoBoundingBoxDocSet extends MatchDocIdSet {
        private final GeoPointValues values;
        private final Point topLeft;
        private final Point bottomRight;

        public GeoBoundingBoxDocSet(int maxDoc, @Nullable Bits acceptDocs, GeoPointValues values, Point topLeft, Point bottomRight) {
            super(maxDoc, acceptDocs);
            this.values = values;
            this.topLeft = topLeft;
            this.bottomRight = bottomRight;
        }

        @Override
        public boolean isCacheable() {
            return true;
        }

        @Override
        protected boolean matchDoc(int doc) {
            if (!values.hasValue(doc)) {
                return false;
            }

            if (values.isMultiValued()) {
                GeoPointValues.Iter iter = values.getIter(doc);
                while (iter.hasNext()) {
                    GeoPoint point = iter.next();
                    if (topLeft.lon <= point.lon() && bottomRight.lon >= point.lon()
                            && topLeft.lat >= point.lat() && bottomRight.lat <= point.lat()) {
                        return true;
                    }
                }
            } else {
                GeoPoint point = values.getValue(doc);
                if (topLeft.lon <= point.lon() && bottomRight.lon >= point.lon()
                        && topLeft.lat >= point.lat() && bottomRight.lat <= point.lat()) {
                    return true;
                }
            }
            return false;
        }
    }
}
