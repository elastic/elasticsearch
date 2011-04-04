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
import org.elasticsearch.index.cache.field.data.FieldDataCache;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.xcontent.geo.GeoPointFieldData;
import org.elasticsearch.index.mapper.xcontent.geo.GeoPointFieldDataType;
import org.elasticsearch.index.search.geo.GeoDistance;
import org.elasticsearch.search.facet.AbstractFacetCollector;
import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.facet.FacetPhaseExecutionException;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;

/**
 * @author kimchy (shay.banon)
 */
public class GeoDistanceFacetCollector extends AbstractFacetCollector {

    protected final String indexFieldName;

    protected final double lat;

    protected final double lon;

    protected final DistanceUnit unit;

    protected final GeoDistance geoDistance;

    protected final FieldDataCache fieldDataCache;

    protected GeoPointFieldData fieldData;

    protected final GeoDistanceFacet.Entry[] entries;

    protected GeoPointFieldData.ValueInDocProc aggregator;

    public GeoDistanceFacetCollector(String facetName, String fieldName, double lat, double lon, DistanceUnit unit, GeoDistance geoDistance,
                                     GeoDistanceFacet.Entry[] entries, SearchContext context) {
        super(facetName);
        this.lat = lat;
        this.lon = lon;
        this.unit = unit;
        this.entries = entries;
        this.geoDistance = geoDistance;
        this.fieldDataCache = context.fieldDataCache();

        MapperService.SmartNameFieldMappers smartMappers = context.mapperService().smartName(fieldName);
        if (smartMappers == null || !smartMappers.hasMapper()) {
            throw new FacetPhaseExecutionException(facetName, "No mapping found for field [" + fieldName + "]");
        }
        if (smartMappers.mapper().fieldDataType() != GeoPointFieldDataType.TYPE) {
            throw new FacetPhaseExecutionException(facetName, "field [" + fieldName + "] is not a geo_point field");
        }

        // add type filter if there is exact doc mapper associated with it
        if (smartMappers.hasDocMapper()) {
            setFilter(context.filterCache().cache(smartMappers.docMapper().typeFilter()));
        }

        this.indexFieldName = smartMappers.mapper().names().indexName();
        this.aggregator = new Aggregator(lat, lon, geoDistance, unit, entries);
    }

    @Override protected void doSetNextReader(IndexReader reader, int docBase) throws IOException {
        fieldData = (GeoPointFieldData) fieldDataCache.cache(GeoPointFieldDataType.TYPE, reader, indexFieldName);
    }

    @Override protected void doCollect(int doc) throws IOException {
        for (GeoDistanceFacet.Entry entry : entries) {
            entry.foundInDoc = false;
        }
        fieldData.forEachValueInDoc(doc, aggregator);
    }

    @Override public Facet facet() {
        return new InternalGeoDistanceFacet(facetName, entries);
    }

    public static class Aggregator implements GeoPointFieldData.ValueInDocProc {

        protected final double lat;

        protected final double lon;

        private final GeoDistance geoDistance;

        private final DistanceUnit unit;

        private final GeoDistanceFacet.Entry[] entries;

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
                    entry.total += distance;
                    if (distance < entry.min) {
                        entry.min = distance;
                    }
                    if (distance > entry.max) {
                        entry.max = distance;
                    }
                }
            }
        }
    }
}
