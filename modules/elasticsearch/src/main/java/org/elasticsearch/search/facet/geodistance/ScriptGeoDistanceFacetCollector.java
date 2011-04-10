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

package org.elasticsearch.search.facet.geodistance;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Scorer;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.index.mapper.xcontent.geo.GeoPointFieldData;
import org.elasticsearch.index.search.geo.GeoDistance;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Map;

/**
 * @author kimchy (shay.banon)
 */
public class ScriptGeoDistanceFacetCollector extends GeoDistanceFacetCollector {

    private final SearchScript script;

    private Aggregator scriptAggregator;

    public ScriptGeoDistanceFacetCollector(String facetName, String fieldName, double lat, double lon, DistanceUnit unit, GeoDistance geoDistance,
                                           GeoDistanceFacet.Entry[] entries, SearchContext context,
                                           String scriptLang, String script, Map<String, Object> params) {
        super(facetName, fieldName, lat, lon, unit, geoDistance, entries, context);

        this.script = context.scriptService().search(context.lookup(), scriptLang, script, params);
        this.aggregator = new Aggregator(lat, lon, geoDistance, unit, entries);
        this.scriptAggregator = (Aggregator) this.aggregator;
    }

    @Override public void setScorer(Scorer scorer) throws IOException {
        script.setScorer(scorer);
    }

    @Override protected void doSetNextReader(IndexReader reader, int docBase) throws IOException {
        super.doSetNextReader(reader, docBase);
        script.setNextReader(reader);
    }

    @Override protected void doCollect(int doc) throws IOException {
        script.setNextDocId(doc);
        this.scriptAggregator.scriptValue = script.runAsDouble();
        super.doCollect(doc);
    }

    public static class Aggregator implements GeoPointFieldData.ValueInDocProc {

        protected final double lat;

        protected final double lon;

        private final GeoDistance geoDistance;

        private final DistanceUnit unit;

        private final GeoDistanceFacet.Entry[] entries;

        double scriptValue;

        public Aggregator(double lat, double lon, GeoDistance geoDistance, DistanceUnit unit, GeoDistanceFacet.Entry[] entries) {
            this.lat = lat;
            this.lon = lon;
            this.geoDistance = geoDistance;
            this.unit = unit;
            this.entries = entries;
        }

        @Override public void onValue(int docId, double lat, double lon) {
            double distance = geoDistance.calculate(this.lat, this.lon, lat, lon, unit);
            for (GeoDistanceFacet.Entry entry : entries) {
                if (entry.foundInDoc) {
                    continue;
                }
                if (distance >= entry.getFrom() && distance < entry.getTo()) {
                    entry.foundInDoc = true;
                    entry.count++;
                    entry.totalCount++;
                    entry.total += scriptValue;
                    if (scriptValue < entry.min) {
                        entry.min = scriptValue;
                    }
                    if (scriptValue > entry.max) {
                        entry.max = scriptValue;
                    }
                }
            }
        }
    }
}
