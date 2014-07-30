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

package org.elasticsearch.search.facet.geodistance;

import java.io.IOException;
import java.util.Map;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.Scorer;
import org.elasticsearch.common.geo.GeoDistance;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.index.fielddata.IndexGeoPointFieldData;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.internal.SearchContext;

/**
 *
 */
public class ScriptGeoDistanceFacetExecutor extends GeoDistanceFacetExecutor {

    private final SearchScript script;

    public ScriptGeoDistanceFacetExecutor(IndexGeoPointFieldData indexFieldData, double lat, double lon, DistanceUnit unit, GeoDistance geoDistance,
                                          GeoDistanceFacet.Entry[] entries, SearchContext context,
                                          String scriptLang, String script, ScriptService.ScriptType scriptType, Map<String, Object> params) {
        super(indexFieldData, lat, lon, unit, geoDistance, entries, context);
        this.script = context.scriptService().search(context.lookup(), scriptLang, script, scriptType, params);
    }

    @Override
    public Collector collector() {
        return new Collector(new ScriptAggregator(fixedSourceDistance, entries, script));
    }

    class Collector extends GeoDistanceFacetExecutor.Collector {


        private ScriptAggregator scriptAggregator;

        Collector(ScriptAggregator aggregator) {
            super(aggregator);
            this.scriptAggregator = aggregator;
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
            scriptAggregator.scriptValue = script.runAsDouble();
            super.collect(doc);
        }
    }

    public final static class ScriptAggregator extends GeoDistanceFacetExecutor.Aggregator{

        private double scriptValue;

        public ScriptAggregator(GeoDistance.FixedSourceDistance fixedSourceDistance, GeoDistanceFacet.Entry[] entries, SearchScript script) {
            super(fixedSourceDistance, entries);
        }
        
        @Override
        protected void collectGeoPoint(GeoDistanceFacet.Entry entry, int docId, double distance) {
            final double scriptValue = this.scriptValue;
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
