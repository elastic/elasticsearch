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
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.index.mapper.xcontent.geo.GeoPoint;
import org.elasticsearch.index.search.geo.GeoDistance;
import org.elasticsearch.script.search.SearchScript;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Map;

/**
 * @author kimchy (shay.banon)
 */
public class ScriptGeoDistanceFacetCollector extends GeoDistanceFacetCollector {

    private final SearchScript script;

    public ScriptGeoDistanceFacetCollector(String facetName, String fieldName, double lat, double lon, DistanceUnit unit, GeoDistance geoDistance,
                                           GeoDistanceFacet.Entry[] entries, SearchContext context,
                                           String scriptLang, String script, Map<String, Object> params) {
        super(facetName, fieldName, lat, lon, unit, geoDistance, entries, context);

        this.script = new SearchScript(context.lookup(), scriptLang, script, params, context.scriptService());
    }

    @Override protected void doSetNextReader(IndexReader reader, int docBase) throws IOException {
        super.doSetNextReader(reader, docBase);
        script.setNextReader(reader);
    }

    @Override protected void doCollect(int doc) throws IOException {
        if (!fieldData.hasValue(doc)) {
            return;
        }

        double value = ((Number) script.execute(doc)).doubleValue();

        if (fieldData.multiValued()) {
            GeoPoint[] points = fieldData.values(doc);
            for (GeoPoint point : points) {
                double distance = geoDistance.calculate(lat, lon, point.lat(), point.lon(), unit);
                for (GeoDistanceFacet.Entry entry : entries) {
                    if (distance >= entry.getFrom() && distance < entry.getTo()) {
                        entry.count++;
                        entry.total += value;
                    }
                }
            }
        } else {
            GeoPoint point = fieldData.value(doc);
            double distance = geoDistance.calculate(lat, lon, point.lat(), point.lon(), unit);
            for (GeoDistanceFacet.Entry entry : entries) {
                if (distance >= entry.getFrom() && distance < entry.getTo()) {
                    entry.count++;
                    entry.total += value;
                }
            }
        }
    }
}
