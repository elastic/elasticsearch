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
import org.elasticsearch.index.field.data.FieldDataType;
import org.elasticsearch.index.field.data.NumericFieldData;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.xcontent.geo.GeoPointFieldData;
import org.elasticsearch.index.search.geo.GeoDistance;
import org.elasticsearch.search.facet.FacetPhaseExecutionException;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;

/**
 * @author kimchy (shay.banon)
 */
public class ValueGeoDistanceFacetCollector extends GeoDistanceFacetCollector {

    private final String indexValueFieldName;

    private final FieldDataType valueFieldDataType;

    public ValueGeoDistanceFacetCollector(String facetName, String fieldName, double lat, double lon, DistanceUnit unit, GeoDistance geoDistance,
                                          GeoDistanceFacet.Entry[] entries, SearchContext context, String valueFieldName) {
        super(facetName, fieldName, lat, lon, unit, geoDistance, entries, context);

        FieldMapper mapper = context.mapperService().smartNameFieldMapper(valueFieldName);
        if (mapper == null) {
            throw new FacetPhaseExecutionException(facetName, "No mapping found for field [" + valueFieldName + "]");
        }
        this.indexValueFieldName = valueFieldName;
        this.valueFieldDataType = mapper.fieldDataType();
        this.aggregator = new Aggregator(lat, lon, geoDistance, unit, entries);
    }

    @Override protected void doSetNextReader(IndexReader reader, int docBase) throws IOException {
        super.doSetNextReader(reader, docBase);
        ((Aggregator) this.aggregator).valueFieldData = (NumericFieldData) fieldDataCache.cache(valueFieldDataType, reader, indexValueFieldName);
    }

    public static class Aggregator implements GeoPointFieldData.ValueInDocProc {

        protected final double lat;

        protected final double lon;

        private final GeoDistance geoDistance;

        private final DistanceUnit unit;

        private final GeoDistanceFacet.Entry[] entries;

        NumericFieldData valueFieldData;

        final ValueAggregator valueAggregator = new ValueAggregator();

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
                    valueAggregator.entry = entry;
                    valueFieldData.forEachValueInDoc(docId, valueAggregator);
                }
            }
        }
    }

    public static class ValueAggregator implements NumericFieldData.DoubleValueInDocProc {

        GeoDistanceFacet.Entry entry;

        @Override public void onValue(int docId, double value) {
            entry.totalCount++;
            entry.total += value;
            if (value < entry.min) {
                entry.min = value;
            }
            if (value > entry.max) {
                entry.max = value;
            }
        }
    }
}
