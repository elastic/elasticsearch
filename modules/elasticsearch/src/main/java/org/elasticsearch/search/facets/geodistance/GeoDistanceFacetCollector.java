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
import org.elasticsearch.index.mapper.xcontent.XContentGeoPointFieldMapper;
import org.elasticsearch.search.facets.Facet;
import org.elasticsearch.search.facets.FacetPhaseExecutionException;
import org.elasticsearch.search.facets.support.AbstractFacetCollector;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;

/**
 * @author kimchy (shay.banon)
 */
public class GeoDistanceFacetCollector extends AbstractFacetCollector {

    protected final String fieldName;

    protected final String indexLatFieldName;

    protected final String indexLonFieldName;

    protected final double lat;

    protected final double lon;

    protected final DistanceUnit unit;

    protected final GeoDistance geoDistance;

    protected final FieldDataCache fieldDataCache;

    protected final FieldData.Type fieldDataType;

    protected NumericFieldData latFieldData;

    protected NumericFieldData lonFieldData;

    protected final GeoDistanceFacet.Entry[] entries;

    public GeoDistanceFacetCollector(String facetName, String fieldName, double lat, double lon, DistanceUnit unit, GeoDistance geoDistance,
                                     GeoDistanceFacet.Entry[] entries, SearchContext context) {
        super(facetName);
        this.fieldName = fieldName;
        this.lat = lat;
        this.lon = lon;
        this.unit = unit;
        this.entries = entries;
        this.geoDistance = geoDistance;
        this.fieldDataCache = context.fieldDataCache();

        MapperService.SmartNameFieldMappers smartMappers = context.mapperService().smartName(fieldName + XContentGeoPointFieldMapper.Names.LAT_SUFFIX);
        if (smartMappers == null || !smartMappers.hasMapper()) {
            throw new FacetPhaseExecutionException(facetName, "No mapping found for field [" + fieldName + "]");
        }

        // add type filter if there is exact doc mapper associated with it
        if (smartMappers.hasDocMapper()) {
            setFilter(context.filterCache().cache(smartMappers.docMapper().typeFilter()));
        }

        this.indexLatFieldName = smartMappers.mapper().names().indexName();

        FieldMapper mapper = context.mapperService().smartNameFieldMapper(fieldName + XContentGeoPointFieldMapper.Names.LON_SUFFIX);
        if (mapper == null) {
            throw new FacetPhaseExecutionException(facetName, "No mapping found for field [" + fieldName + "]");
        }
        this.indexLonFieldName = mapper.names().indexName();
        this.fieldDataType = mapper.fieldDataType();
    }

    @Override protected void doSetNextReader(IndexReader reader, int docBase) throws IOException {
        latFieldData = (NumericFieldData) fieldDataCache.cache(fieldDataType, reader, indexLatFieldName);
        lonFieldData = (NumericFieldData) fieldDataCache.cache(fieldDataType, reader, indexLonFieldName);
    }

    @Override protected void doCollect(int doc) throws IOException {
        if (!latFieldData.hasValue(doc) || !lonFieldData.hasValue(doc)) {
            return;
        }

        if (latFieldData.multiValued()) {
            double[] lats = latFieldData.doubleValues(doc);
            double[] lons = latFieldData.doubleValues(doc);
            for (int i = 0; i < lats.length; i++) {
                double distance = geoDistance.calculate(lat, lon, lats[i], lons[i], unit);
                for (GeoDistanceFacet.Entry entry : entries) {
                    if (distance >= entry.getFrom() && distance < entry.getTo()) {
                        entry.count++;
                        entry.total += distance;
                    }
                }
            }
        } else {
            double distance = geoDistance.calculate(lat, lon, latFieldData.doubleValue(doc), lonFieldData.doubleValue(doc), unit);
            for (GeoDistanceFacet.Entry entry : entries) {
                if (distance >= entry.getFrom() && distance < entry.getTo()) {
                    entry.count++;
                    entry.total += distance;
                }
            }
        }
    }

    @Override public Facet facet() {
        return new InternalGeoDistanceFacet(facetName, fieldName, fieldName, unit, entries);
    }
}
