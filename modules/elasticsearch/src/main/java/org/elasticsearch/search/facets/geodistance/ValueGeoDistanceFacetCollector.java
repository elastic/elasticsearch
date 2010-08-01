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

package org.elasticsearch.search.facets.geodistance;

import org.apache.lucene.index.IndexReader;
import org.elasticsearch.common.lucene.geo.GeoDistance;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.index.cache.field.data.FieldDataCache;
import org.elasticsearch.index.field.data.FieldData;
import org.elasticsearch.index.field.data.NumericFieldData;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.search.facets.Facet;
import org.elasticsearch.search.facets.FacetPhaseExecutionException;

import java.io.IOException;

/**
 * @author kimchy (shay.banon)
 */
public class ValueGeoDistanceFacetCollector extends GeoDistanceFacetCollector {

    private final String valueFieldName;

    private final String indexValueFieldName;

    private final FieldData.Type valueFieldDataType;

    private NumericFieldData valueFieldData;

    public ValueGeoDistanceFacetCollector(String facetName, String fieldName, double lat, double lon, DistanceUnit unit, GeoDistance geoDistance,
                                          GeoDistanceFacet.Entry[] entries, FieldDataCache fieldDataCache, MapperService mapperService, String valueFieldName) {
        super(facetName, fieldName, lat, lon, unit, geoDistance, entries, fieldDataCache, mapperService);
        this.valueFieldName = valueFieldName;

        FieldMapper mapper = mapperService.smartNameFieldMapper(valueFieldName);
        if (mapper == null) {
            throw new FacetPhaseExecutionException(facetName, "No mapping found for field [" + valueFieldName + "]");
        }
        this.indexValueFieldName = valueFieldName;
        this.valueFieldDataType = mapper.fieldDataType();
    }

    @Override protected void doSetNextReader(IndexReader reader, int docBase) throws IOException {
        super.doSetNextReader(reader, docBase);
        valueFieldData = (NumericFieldData) fieldDataCache.cache(valueFieldDataType, reader, indexValueFieldName);
    }

    @Override protected void doCollect(int doc) throws IOException {
        if (!latFieldData.hasValue(doc) || !lonFieldData.hasValue(doc)) {
            return;
        }

        if (latFieldData.multiValued()) {
            double[] lats = latFieldData.doubleValues(doc);
            double[] lons = latFieldData.doubleValues(doc);
            double[] values = valueFieldData.multiValued() ? valueFieldData.doubleValues(doc) : null;
            for (int i = 0; i < lats.length; i++) {
                double distance = geoDistance.calculate(lat, lon, lats[i], lons[i], unit);
                for (GeoDistanceFacet.Entry entry : entries) {
                    if (distance >= entry.getFrom() && distance < entry.getTo()) {
                        entry.count++;
                        if (values != null) {
                            if (i < values.length) {
                                entry.total += values[i];
                            }
                        } else if (valueFieldData.hasValue(doc)) {
                            entry.total += valueFieldData.doubleValue(doc);
                        }
                    }
                }
            }
        } else {
            double distance = geoDistance.calculate(lat, lon, latFieldData.doubleValue(doc), lonFieldData.doubleValue(doc), unit);
            for (GeoDistanceFacet.Entry entry : entries) {
                if (distance >= entry.getFrom() && distance < entry.getTo()) {
                    entry.count++;
                    if (valueFieldData.multiValued()) {
                        double[] values = valueFieldData.doubleValues(doc);
                        for (double value : values) {
                            entry.total += value;
                        }
                    } else if (valueFieldData.hasValue(doc)) {
                        entry.total += valueFieldData.doubleValue(doc);
                    }
                }
            }
        }
    }

    @Override public Facet facet() {
        return new InternalGeoDistanceFacet(facetName, fieldName, valueFieldName, unit, entries);
    }
}
