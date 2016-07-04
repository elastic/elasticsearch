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

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.RandomAccessWeight;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.index.fielddata.IndexGeoPointFieldData;
import org.elasticsearch.index.fielddata.MultiGeoPointValues;

import java.io.IOException;
import java.util.Objects;

/**
 *
 */
public class InMemoryGeoBoundingBoxQuery extends Query {

    private final GeoPoint topLeft;
    private final GeoPoint bottomRight;

    private final IndexGeoPointFieldData indexFieldData;

    public InMemoryGeoBoundingBoxQuery(GeoPoint topLeft, GeoPoint bottomRight, IndexGeoPointFieldData indexFieldData) {
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
        return indexFieldData.getFieldName();
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {
        return new RandomAccessWeight(this) {
            @Override
            protected Bits getMatchingDocs(LeafReaderContext context) throws IOException {
                final int maxDoc = context.reader().maxDoc();
                final MultiGeoPointValues values = indexFieldData.load(context).getGeoPointValues();
                // checks to see if bounding box crosses 180 degrees
                if (topLeft.lon() > bottomRight.lon()) {
                    return new Meridian180GeoBoundingBoxBits(maxDoc, values, topLeft, bottomRight);
                } else {
                    return new GeoBoundingBoxBits(maxDoc, values, topLeft, bottomRight);
                }
            }
        };
    }

    @Override
    public String toString(String field) {
        return "GeoBoundingBoxFilter(" + indexFieldData.getFieldName() + ", " + topLeft + ", " + bottomRight + ")";
    }

    @Override
    public boolean equals(Object obj) {
        if (sameClassAs(obj) == false) {
            return false;
        }
        InMemoryGeoBoundingBoxQuery other = (InMemoryGeoBoundingBoxQuery) obj;
        return fieldName().equalsIgnoreCase(other.fieldName())
                && topLeft.equals(other.topLeft)
                && bottomRight.equals(other.bottomRight);
    }

    @Override
    public int hashCode() {
        return Objects.hash(classHash(), fieldName(), topLeft, bottomRight);
    }

    private static class Meridian180GeoBoundingBoxBits implements Bits {
        private final int maxDoc;
        private final MultiGeoPointValues values;
        private final GeoPoint topLeft;
        private final GeoPoint bottomRight;

        public Meridian180GeoBoundingBoxBits(int maxDoc, MultiGeoPointValues values, GeoPoint topLeft, GeoPoint bottomRight) {
            this.maxDoc = maxDoc;
            this.values = values;
            this.topLeft = topLeft;
            this.bottomRight = bottomRight;
        }

        @Override
        public boolean get(int doc) {
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

        @Override
        public int length() {
            return maxDoc;
        }
    }

    private static class GeoBoundingBoxBits implements Bits {
        private final int maxDoc;
        private final MultiGeoPointValues values;
        private final GeoPoint topLeft;
        private final GeoPoint bottomRight;

        public GeoBoundingBoxBits(int maxDoc, MultiGeoPointValues values, GeoPoint topLeft, GeoPoint bottomRight) {
            this.maxDoc = maxDoc;
            this.values = values;
            this.topLeft = topLeft;
            this.bottomRight = bottomRight;
        }

        @Override
        public boolean get(int doc) {
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

        @Override
        public int length() {
            return maxDoc;
        }
    }
}
