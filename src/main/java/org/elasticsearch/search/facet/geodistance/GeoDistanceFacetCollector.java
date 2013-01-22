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

import org.apache.lucene.index.AtomicReaderContext;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.index.fielddata.GeoPointValues;
import org.elasticsearch.index.fielddata.IndexGeoPointFieldData;
import org.elasticsearch.index.search.geo.GeoDistance;
import org.elasticsearch.search.facet.AbstractFacetCollector;
import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;

/**
 *
 */
public class GeoDistanceFacetCollector extends AbstractFacetCollector {

    protected final IndexGeoPointFieldData indexFieldData;

    protected final double lat;
    protected final double lon;

    protected final DistanceUnit unit;

    protected final GeoDistance geoDistance;
    protected final GeoDistance.FixedSourceDistance fixedSourceDistance;

    protected GeoPointValues values;

    protected final GeoDistanceFacet.Entry[] entries;
    protected GeoPointValues.LatLonValueInDocProc aggregator;

    public GeoDistanceFacetCollector(String facetName, IndexGeoPointFieldData indexFieldData, double lat, double lon, DistanceUnit unit, GeoDistance geoDistance,
                                     GeoDistanceFacet.Entry[] entries, SearchContext context) {
        super(facetName);
        this.lat = lat;
        this.lon = lon;
        this.unit = unit;
        this.entries = entries;
        this.geoDistance = geoDistance;
        this.indexFieldData = indexFieldData;
        this.fixedSourceDistance = geoDistance.fixedSourceDistance(lat, lon, unit);
        this.aggregator = new Aggregator(fixedSourceDistance, entries);
    }

    @Override
    protected void doSetNextReader(AtomicReaderContext context) throws IOException {
        values = indexFieldData.load(context).getGeoPointValues();
    }

    @Override
    protected void doCollect(int doc) throws IOException {
        for (GeoDistanceFacet.Entry entry : entries) {
            entry.foundInDoc = false;
        }
        values.forEachLatLonValueInDoc(doc, aggregator);
    }

    @Override
    public Facet facet() {
        return new InternalGeoDistanceFacet(facetName, entries);
    }

    public static class Aggregator implements GeoPointValues.LatLonValueInDocProc {

        private final GeoDistance.FixedSourceDistance fixedSourceDistance;

        private final GeoDistanceFacet.Entry[] entries;

        public Aggregator(GeoDistance.FixedSourceDistance fixedSourceDistance, GeoDistanceFacet.Entry[] entries) {
            this.fixedSourceDistance = fixedSourceDistance;
            this.entries = entries;
        }

        @Override
        public void onMissing(int docId) {
        }

        @Override
        public void onValue(int docId, double lat, double lon) {
            double distance = fixedSourceDistance.calculate(lat, lon);
            for (GeoDistanceFacet.Entry entry : entries) {
                if (entry.foundInDoc) {
                    continue;
                }
                if (distance >= entry.getFrom() && distance < entry.getTo()) {
                    entry.foundInDoc = true;
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
    }
}
