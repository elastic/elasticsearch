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
import org.apache.lucene.search.Scorer;
import org.elasticsearch.common.geo.GeoDistance;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.index.fielddata.GeoPointValues;
import org.elasticsearch.index.fielddata.IndexGeoPointFieldData;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Map;

/**
 *
 */
public class ScriptGeoDistanceFacetExecutor extends GeoDistanceFacetExecutor {

    private final SearchScript script;

    public ScriptGeoDistanceFacetExecutor(IndexGeoPointFieldData indexFieldData, double lat, double lon, DistanceUnit unit, GeoDistance geoDistance,
                                          GeoDistanceFacet.Entry[] entries, SearchContext context,
                                          String scriptLang, String script, Map<String, Object> params) {
        super(indexFieldData, lat, lon, unit, geoDistance, entries, context);
        this.script = context.scriptService().search(context.lookup(), scriptLang, script, params);
    }

    @Override
    public Collector collector() {
        return new Collector();
    }

    class Collector extends GeoDistanceFacetExecutor.Collector {

        private Aggregator scriptAggregator;

        Collector() {
            this.aggregator = new Aggregator(fixedSourceDistance, entries);
            this.scriptAggregator = (Aggregator) this.aggregator;
        }

        @Override
        public void setScorer(Scorer scorer) throws IOException {
            script.setScorer(scorer);
        }

        @Override
        public void setNextReader(AtomicReaderContext context) throws IOException {
            super.setNextReader(context);
            script.setNextReader(context);
        }

        @Override
        public void collect(int doc) throws IOException {
            script.setNextDocId(doc);
            this.scriptAggregator.scriptValue = script.runAsDouble();
            super.collect(doc);
        }
    }

    public static class Aggregator implements GeoPointValues.LatLonValueInDocProc {

        private final GeoDistance.FixedSourceDistance fixedSourceDistance;

        private final GeoDistanceFacet.Entry[] entries;

        double scriptValue;

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
