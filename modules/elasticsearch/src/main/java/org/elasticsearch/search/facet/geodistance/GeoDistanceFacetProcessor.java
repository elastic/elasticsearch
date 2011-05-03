/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.facet.geodistance;

import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.xcontent.geo.GeoPointFieldMapper;
import org.elasticsearch.index.search.geo.GeoDistance;
import org.elasticsearch.index.search.geo.GeoHashUtils;
import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.facet.FacetCollector;
import org.elasticsearch.search.facet.FacetPhaseExecutionException;
import org.elasticsearch.search.facet.FacetProcessor;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * @author kimchy (shay.banon)
 */
public class GeoDistanceFacetProcessor extends AbstractComponent implements FacetProcessor {

    @Inject public GeoDistanceFacetProcessor(Settings settings) {
        super(settings);
        InternalGeoDistanceFacet.registerStreams();
    }

    @Override public String[] types() {
        return new String[]{GeoDistanceFacet.TYPE, "geoDistance"};
    }

    @Override public FacetCollector parse(String facetName, XContentParser parser, SearchContext context) throws IOException {
        String fieldName = null;
        String valueFieldName = null;
        String valueScript = null;
        String scriptLang = null;
        Map<String, Object> params = null;
        double lat = Double.NaN;
        double lon = Double.NaN;
        DistanceUnit unit = DistanceUnit.KILOMETERS;
        GeoDistance geoDistance = GeoDistance.ARC;
        List<GeoDistanceFacet.Entry> entries = Lists.newArrayList();

        XContentParser.Token token;
        String currentName = parser.currentName();

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentName = parser.currentName();
            } else if (token == XContentParser.Token.START_ARRAY) {
                if ("ranges".equals(currentName) || "entries".equals(currentName)) {
                    // "ranges" : [
                    //     { "from" : 0, "to" : 12.5 }
                    //     { "from" : 12.5 }
                    // ]
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        double from = Double.NEGATIVE_INFINITY;
                        double to = Double.POSITIVE_INFINITY;
                        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                            if (token == XContentParser.Token.FIELD_NAME) {
                                currentName = parser.currentName();
                            } else if (token.isValue()) {
                                if ("from".equals(currentName)) {
                                    from = parser.doubleValue();
                                } else if ("to".equals(currentName)) {
                                    to = parser.doubleValue();
                                }
                            }
                        }
                        entries.add(new GeoDistanceFacet.Entry(from, to, 0, 0, 0, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY));
                    }
                } else {
                    token = parser.nextToken();
                    lon = parser.doubleValue();
                    token = parser.nextToken();
                    lat = parser.doubleValue();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {

                    }
                    fieldName = currentName;
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("params".equals(currentName)) {
                    params = parser.map();
                } else {
                    // the json in the format of -> field : { lat : 30, lon : 12 }
                    fieldName = currentName;
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            currentName = parser.currentName();
                        } else if (token.isValue()) {
                            if (currentName.equals(GeoPointFieldMapper.Names.LAT)) {
                                lat = parser.doubleValue();
                            } else if (currentName.equals(GeoPointFieldMapper.Names.LON)) {
                                lon = parser.doubleValue();
                            } else if (currentName.equals(GeoPointFieldMapper.Names.GEOHASH)) {
                                double[] values = GeoHashUtils.decode(parser.text());
                                lat = values[0];
                                lon = values[1];
                            }
                        }
                    }
                }
            } else if (token.isValue()) {
                if (currentName.equals("unit")) {
                    unit = DistanceUnit.fromString(parser.text());
                } else if (currentName.equals("distance_type") || currentName.equals("distanceType")) {
                    geoDistance = GeoDistance.fromString(parser.text());
                } else if ("value_field".equals(currentName) || "valueField".equals(currentName)) {
                    valueFieldName = parser.text();
                } else if ("value_script".equals(currentName) || "valueScript".equals(currentName)) {
                    valueScript = parser.text();
                } else if ("lang".equals(currentName)) {
                    scriptLang = parser.text();
                } else {
                    // assume the value is the actual value
                    String value = parser.text();
                    int comma = value.indexOf(',');
                    if (comma != -1) {
                        lat = Double.parseDouble(value.substring(0, comma).trim());
                        lon = Double.parseDouble(value.substring(comma + 1).trim());
                    } else {
                        double[] values = GeoHashUtils.decode(value);
                        lat = values[0];
                        lon = values[1];
                    }

                    fieldName = currentName;
                }
            }
        }

        if (Double.isNaN(lat) || Double.isNaN(lon)) {
            throw new FacetPhaseExecutionException(facetName, "lat/lon not set for geo_distance facet");
        }

        if (entries.isEmpty()) {
            throw new FacetPhaseExecutionException(facetName, "no ranges defined for geo_distance facet");
        }

        if (valueFieldName != null) {
            return new ValueGeoDistanceFacetCollector(facetName, fieldName, lat, lon, unit, geoDistance, entries.toArray(new GeoDistanceFacet.Entry[entries.size()]),
                    context, valueFieldName);
        }

        if (valueScript != null) {
            return new ScriptGeoDistanceFacetCollector(facetName, fieldName, lat, lon, unit, geoDistance, entries.toArray(new GeoDistanceFacet.Entry[entries.size()]),
                    context, scriptLang, valueScript, params);
        }

        return new GeoDistanceFacetCollector(facetName, fieldName, lat, lon, unit, geoDistance, entries.toArray(new GeoDistanceFacet.Entry[entries.size()]),
                context);
    }

    @Override public Facet reduce(String name, List<Facet> facets) {
        InternalGeoDistanceFacet agg = null;
        for (Facet facet : facets) {
            InternalGeoDistanceFacet geoDistanceFacet = (InternalGeoDistanceFacet) facet;
            if (agg == null) {
                agg = geoDistanceFacet;
            } else {
                for (int i = 0; i < geoDistanceFacet.entries.length; i++) {
                    GeoDistanceFacet.Entry aggEntry = agg.entries[i];
                    GeoDistanceFacet.Entry currentEntry = geoDistanceFacet.entries[i];
                    aggEntry.count += currentEntry.count;
                    aggEntry.totalCount += currentEntry.totalCount;
                    aggEntry.total += currentEntry.total;
                    if (currentEntry.min < aggEntry.min) {
                        aggEntry.min = currentEntry.min;
                    }
                    if (currentEntry.max > aggEntry.max) {
                        aggEntry.max = currentEntry.max;
                    }
                }
            }
        }
        return agg;
    }
}
