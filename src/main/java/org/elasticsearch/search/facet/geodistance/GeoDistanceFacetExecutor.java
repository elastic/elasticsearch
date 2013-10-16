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

package org.elasticsearch.search.facet.geodistance;

import java.io.IOException;

import org.apache.lucene.index.AtomicReaderContext;
import org.elasticsearch.common.geo.GeoDistance;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.index.fielddata.GeoPointValues;
import org.elasticsearch.index.fielddata.IndexGeoPointFieldData;
import org.elasticsearch.search.facet.FacetExecutor;
import org.elasticsearch.search.facet.InternalFacet;
import org.elasticsearch.search.internal.SearchContext;

/**
 *
 */
public class GeoDistanceFacetExecutor extends FacetExecutor {

    final IndexGeoPointFieldData indexFieldData;
    final double lat;
    final double lon;
    final DistanceUnit unit;
    final GeoDistance geoDistance;
    final GeoDistance.FixedSourceDistance fixedSourceDistance;

    final GeoDistanceFacet.Entry[] entries;

    public GeoDistanceFacetExecutor(IndexGeoPointFieldData indexFieldData, double lat, double lon, DistanceUnit unit, GeoDistance geoDistance,
                                    GeoDistanceFacet.Entry[] entries, SearchContext context) {
        this.lat = lat;
        this.lon = lon;
        this.unit = unit;
        this.entries = entries;
        this.geoDistance = geoDistance;
        this.indexFieldData = indexFieldData;
        this.fixedSourceDistance = geoDistance.fixedSourceDistance(lat, lon, unit);
    }

    @Override
    public Collector collector() {
        return new Collector(new Aggregator(fixedSourceDistance, entries));
    }

    @Override
    public InternalFacet buildFacet(String facetName) {
        return new InternalGeoDistanceFacet(facetName, entries);
    }

    class Collector extends FacetExecutor.Collector {

        protected GeoPointValues values;
        protected final Aggregator aggregator;

        Collector(Aggregator aggregator) {
            this.aggregator = aggregator;
        }

        @Override
        public void setNextReader(AtomicReaderContext context) throws IOException {
            values = indexFieldData.load(context).getGeoPointValues();
        }

        @Override
        public void collect(int doc) throws IOException {
            for (GeoDistanceFacet.Entry entry : entries) {
                entry.foundInDoc = false;
            }
            this.aggregator.onDoc(doc, values);
        }

        @Override
        public void postCollection() {
        }
    }

    public static class Aggregator {

        protected final GeoDistance.FixedSourceDistance fixedSourceDistance;

        protected final GeoDistanceFacet.Entry[] entries;

        public Aggregator(GeoDistance.FixedSourceDistance fixedSourceDistance, GeoDistanceFacet.Entry[] entries) {
            this.fixedSourceDistance = fixedSourceDistance;
            this.entries = entries;
        }
        
        public void onDoc(int docId, GeoPointValues values) {
            final int length = values.setDocument(docId);
            for (int i = 0; i < length; i++) {
                final GeoPoint next = values.nextValue();
                double distance = fixedSourceDistance.calculate(next.getLat(), next.getLon());
                for (GeoDistanceFacet.Entry entry : entries) {
                    if (entry.foundInDoc) {
                        continue;
                    }
                    if (distance >= entry.getFrom() && distance < entry.getTo()) {
                       entry.foundInDoc = true;
                       collectGeoPoint(entry, docId, distance);
                    }
                }
            }
        }
        
        protected void collectGeoPoint(GeoDistanceFacet.Entry entry, int docId, double distance) {
            entry.count++;
            entry.totalCount++;
            entry.total += distance;
            if (distance < entry.min) {
                entry.min = distance;
            }
            if (distance > entry.max) {
                entry.max = distance;
            }
        }
        
    }
}
