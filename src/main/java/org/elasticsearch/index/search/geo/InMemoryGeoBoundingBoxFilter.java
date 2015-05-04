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

import java.io.IOException;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocValuesDocIdSet;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.Bits;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.index.fielddata.IndexGeoPointFieldData;
import org.elasticsearch.index.fielddata.MultiGeoPointValues;

/**
 *
 */
public class InMemoryGeoBoundingBoxFilter extends Filter {

    private final GeoPoint topLeft;
    private final GeoPoint bottomRight;

    private final IndexGeoPointFieldData indexFieldData;

    public InMemoryGeoBoundingBoxFilter(GeoPoint topLeft, GeoPoint bottomRight, IndexGeoPointFieldData indexFieldData) {
        this.topLeft = topLeft;
        this.bottomRight = bottomRight;
        this.indexFieldData = indexFieldData;
    }

    public GeoPoint topLeft() {
        return topLeft;
    }

    public GeoPoint bottomRight() {
        return bottomRight;
    }

    public String fieldName() {
        return indexFieldData.getFieldNames().indexName();
    }

    @Override
    public DocIdSet getDocIdSet(LeafReaderContext context, Bits acceptedDocs) throws IOException {
        final MultiGeoPointValues values = indexFieldData.load(context).getGeoPointValues();

        //checks to see if bounding box crosses 180 degrees
        if (topLeft.lon() > bottomRight.lon()) {
            return new Meridian180GeoBoundingBoxDocSet(context.reader().maxDoc(), acceptedDocs, values, topLeft, bottomRight);
        } else {
            return new GeoBoundingBoxDocSet(context.reader().maxDoc(), acceptedDocs, values, topLeft, bottomRight);
        }
    }

    @Override
    public String toString(String field) {
        return "GeoBoundingBoxFilter(" + indexFieldData.getFieldNames().indexName() + ", " + topLeft + ", " + bottomRight + ")";
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj) == false) {
            return false;
        }
        InMemoryGeoBoundingBoxFilter other = (InMemoryGeoBoundingBoxFilter) obj;
        return fieldName().equalsIgnoreCase(other.fieldName())
                && topLeft.equals(other.topLeft)
                && bottomRight.equals(other.bottomRight);
    }

    @Override
    public int hashCode() {
        int h = super.hashCode();
        h = 31 * h + fieldName().hashCode();
        h = 31 * h + topLeft.hashCode();
        h = 31 * h + bottomRight.hashCode();
        return h;
    }

    public static class Meridian180GeoBoundingBoxDocSet extends DocValuesDocIdSet {
        private final MultiGeoPointValues values;
        private final GeoPoint topLeft;
        private final GeoPoint bottomRight;

        public Meridian180GeoBoundingBoxDocSet(int maxDoc, @Nullable Bits acceptDocs, MultiGeoPointValues values, GeoPoint topLeft, GeoPoint bottomRight) {
            super(maxDoc, acceptDocs);
            this.values = values;
            this.topLeft = topLeft;
            this.bottomRight = bottomRight;
        }

        @Override
        protected boolean matchDoc(int doc) {
            values.setDocument(doc);
            final int length = values.count();
            for (int i = 0; i < length; i++) {
                GeoPoint point = values.valueAt(i);
                if (((topLeft.lon() <= point.lon() || bottomRight.lon() >= point.lon())) &&
                        (topLeft.lat() >= point.lat() && bottomRight.lat() <= point.lat())) {
                    return true;
                }
            }
            return false;
        }
    }

    public static class GeoBoundingBoxDocSet extends DocValuesDocIdSet {
        private final MultiGeoPointValues values;
        private final GeoPoint topLeft;
        private final GeoPoint bottomRight;

        public GeoBoundingBoxDocSet(int maxDoc, @Nullable Bits acceptDocs, MultiGeoPointValues values, GeoPoint topLeft, GeoPoint bottomRight) {
            super(maxDoc, acceptDocs);
            this.values = values;
            this.topLeft = topLeft;
            this.bottomRight = bottomRight;
        }

        @Override
        protected boolean matchDoc(int doc) {
            values.setDocument(doc);
            final int length = values.count();
            for (int i = 0; i < length; i++) {
                GeoPoint point = values.valueAt(i);
                if (topLeft.lon() <= point.lon() && bottomRight.lon() >= point.lon()
                        && topLeft.lat() >= point.lat() && bottomRight.lat() <= point.lat()) {
                    return true;
                }
            }
            return false;
        }
    }
}
