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

import org.apache.lucene.index.IndexReader;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.index.field.data.FieldDataType;
import org.elasticsearch.index.field.data.NumericFieldData;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.geo.GeoPointFieldData;
import org.elasticsearch.index.search.geo.GeoDistance;
import org.elasticsearch.search.facet.FacetPhaseExecutionException;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;

/**
 *
 */
public class ValueGeoDistanceFacetCollector extends GeoDistanceFacetCollector {

    private final String indexValueFieldName;

    private final FieldDataType valueFieldDataType;

    public ValueGeoDistanceFacetCollector(String facetName, String fieldName, double lat, double lon, DistanceUnit unit, GeoDistance geoDistance,
                                          GeoDistanceFacet.Entry[] entries, SearchContext context, String valueFieldName) {
        super(facetName, fieldName, lat, lon, unit, geoDistance, entries, context);

        MapperService.SmartNameFieldMappers smartMappers = context.smartFieldMappers(valueFieldName);
        if (smartMappers == null || !smartMappers.hasMapper()) {
            throw new FacetPhaseExecutionException(facetName, "No mapping found for field [" + valueFieldName + "]");
        }
        this.indexValueFieldName = smartMappers.mapper().names().indexName();
        this.valueFieldDataType = smartMappers.mapper().fieldDataType();
        this.aggregator = new Aggregator(fixedSourceDistance, entries);
    }

    @Override
    protected void doSetNextReader(IndexReader reader, int docBase) throws IOException {
        super.doSetNextReader(reader, docBase);
        ((Aggregator) this.aggregator).valueFieldData = (NumericFieldData) fieldDataCache.cache(valueFieldDataType, reader, indexValueFieldName);
    }

    public static class Aggregator implements GeoPointFieldData.ValueInDocProc {

        private final GeoDistance.FixedSourceDistance fixedSourceDistance;
        private final GeoDistanceFacet.Entry[] entries;

        NumericFieldData valueFieldData;

        final ValueAggregator valueAggregator = new ValueAggregator();

        public Aggregator(GeoDistance.FixedSourceDistance fixedSourceDistance, GeoDistanceFacet.Entry[] entries) {
            this.fixedSourceDistance = fixedSourceDistance;
            this.entries = entries;
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
                    valueAggregator.entry = entry;
                    valueFieldData.forEachValueInDoc(docId, valueAggregator);
                }
            }
        }
    }

    public static class ValueAggregator implements NumericFieldData.DoubleValueInDocProc {

        GeoDistanceFacet.Entry entry;

        @Override
        public void onValue(int docId, double value) {
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
