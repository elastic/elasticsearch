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

package org.elasticsearch.index.fielddata.fieldcomparator;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.FieldComparator;
import org.elasticsearch.common.geo.GeoDistance;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.index.fielddata.GeoPointValues;
import org.elasticsearch.index.fielddata.IndexGeoPointFieldData;

import java.io.IOException;

/**
 */
public class GeoDistanceComparator extends FieldComparator<Double> {

    protected final IndexGeoPointFieldData<?> indexFieldData;

    protected final double lat;
    protected final double lon;
    protected final DistanceUnit unit;
    protected final GeoDistance geoDistance;
    protected final GeoDistance.FixedSourceDistance fixedSourceDistance;
    protected final SortMode sortMode;

    private final double[] values;
    private double bottom;

    private GeoDistanceValues geoDistanceValues;

    public GeoDistanceComparator(int numHits, IndexGeoPointFieldData<?> indexFieldData, double lat, double lon, DistanceUnit unit, GeoDistance geoDistance, SortMode sortMode) {
        this.values = new double[numHits];
        this.indexFieldData = indexFieldData;
        this.lat = lat;
        this.lon = lon;
        this.unit = unit;
        this.geoDistance = geoDistance;
        this.fixedSourceDistance = geoDistance.fixedSourceDistance(lat, lon, unit);
        this.sortMode = sortMode;
    }

    @Override
    public FieldComparator<Double> setNextReader(AtomicReaderContext context) throws IOException {
        GeoPointValues readerValues = indexFieldData.load(context).getGeoPointValues();
        if (readerValues.isMultiValued()) {
            geoDistanceValues = new MV(readerValues, fixedSourceDistance, sortMode);
        } else {
            geoDistanceValues = new SV(readerValues, fixedSourceDistance);
        }
        return this;
    }

    @Override
    public int compare(int slot1, int slot2) {
        final double v1 = values[slot1];
        final double v2 = values[slot2];
        if (v1 > v2) {
            return 1;
        } else if (v1 < v2) {
            return -1;
        } else {
            return 0;
        }
    }

    @Override
    public int compareBottom(int doc) {
        final double v2 = geoDistanceValues.computeDistance(doc);
        if (bottom > v2) {
            return 1;
        } else if (bottom < v2) {
            return -1;
        } else {
            return 0;
        }
    }

    @Override
    public int compareDocToValue(int doc, Double distance2) throws IOException {
        double distance1 = geoDistanceValues.computeDistance(doc);
        if (distance1 < distance2) {
            return -1;
        } else if (distance1 == distance2) {
            return 0;
        } else {
            return 1;
        }
    }

    @Override
    public void copy(int slot, int doc) {
        values[slot] = geoDistanceValues.computeDistance(doc);
    }

    @Override
    public void setBottom(final int bottom) {
        this.bottom = values[bottom];
    }

    @Override
    public Double value(int slot) {
        return values[slot];
    }

    // Computes the distance based on geo points.
    // Due to this abstractions the geo distance comparator doesn't need to deal with whether fields have one
    // or multiple geo points per document.
    private static abstract class GeoDistanceValues {

        protected final GeoPointValues readerValues;
        protected final GeoDistance.FixedSourceDistance fixedSourceDistance;

        protected GeoDistanceValues(GeoPointValues readerValues, GeoDistance.FixedSourceDistance fixedSourceDistance) {
            this.readerValues = readerValues;
            this.fixedSourceDistance = fixedSourceDistance;
        }

        public abstract double computeDistance(int doc);

    }

    // Deals with one geo point per document
    private static final class SV extends GeoDistanceValues {

        SV(GeoPointValues readerValues, GeoDistance.FixedSourceDistance fixedSourceDistance) {
            super(readerValues, fixedSourceDistance);
        }

        @Override
        public double computeDistance(int doc) {
            GeoPoint geoPoint = readerValues.getValue(doc);
            if (geoPoint == null) {
                // is this true? push this to the "end"
                return Double.MAX_VALUE;
            } else {
                return fixedSourceDistance.calculate(geoPoint.lat(), geoPoint.lon());
            }
        }
    }

    // Deals with more than one geo point per document
    private static final class MV extends GeoDistanceValues {

        private final SortMode sortMode;

        MV(GeoPointValues readerValues, GeoDistance.FixedSourceDistance fixedSourceDistance, SortMode sortMode) {
            super(readerValues, fixedSourceDistance);
            this.sortMode = sortMode;
        }

        @Override
        public double computeDistance(int doc) {
            GeoPointValues.Iter iter = readerValues.getIter(doc);
            if (!iter.hasNext()) {
                return Double.MAX_VALUE;
            }

            GeoPoint point = iter.next();
            double distance = fixedSourceDistance.calculate(point.lat(), point.lon());
            while (iter.hasNext()) {
                point = iter.next();
                double newDistance = fixedSourceDistance.calculate(point.lat(), point.lon());
                switch (sortMode) {
                    case MIN:
                        if (distance > newDistance) {
                            distance = newDistance;
                        }
                        break;
                    case MAX:
                        if (distance < newDistance) {
                            distance = newDistance;
                        }
                        break;
                }
            }
            return distance;
        }

    }

}
