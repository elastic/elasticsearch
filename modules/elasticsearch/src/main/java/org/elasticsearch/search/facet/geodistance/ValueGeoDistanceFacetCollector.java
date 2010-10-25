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
import org.elasticsearch.index.mapper.xcontent.geo.GeoPoint;
import org.elasticsearch.index.search.geo.GeoDistance;
import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.facet.FacetPhaseExecutionException;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;

/**
 * @author kimchy (shay.banon)
 */
public class ValueGeoDistanceFacetCollector extends GeoDistanceFacetCollector {

    private final String valueFieldName;

    private final String indexValueFieldName;

    private final FieldDataType valueFieldDataType;

    private NumericFieldData valueFieldData;

    public ValueGeoDistanceFacetCollector(String facetName, String fieldName, double lat, double lon, DistanceUnit unit, GeoDistance geoDistance,
                                          GeoDistanceFacet.Entry[] entries, SearchContext context, String valueFieldName) {
        super(facetName, fieldName, lat, lon, unit, geoDistance, entries, context);
        this.valueFieldName = valueFieldName;

        FieldMapper mapper = context.mapperService().smartNameFieldMapper(valueFieldName);
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
        if (!fieldData.hasValue(doc)) {
            return;
        }

        if (fieldData.multiValued()) {
            GeoPoint[] points = fieldData.values(doc);
            double[] values = valueFieldData.multiValued() ? valueFieldData.doubleValues(doc) : null;
            for (int i = 0; i < points.length; i++) {
                double distance = geoDistance.calculate(lat, lon, points[i].lat(), points[i].lon(), unit);
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
            GeoPoint point = fieldData.value(doc);
            double distance = geoDistance.calculate(lat, lon, point.lat(), point.lon(), unit);
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
