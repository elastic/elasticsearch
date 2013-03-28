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

package org.elasticsearch.index.fielddata.fieldcomparator;

import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.SortField;
import org.elasticsearch.common.geo.GeoDistance;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexGeoPointFieldData;

import java.io.IOException;

/**
 */
public class GeoDistanceComparatorSource extends IndexFieldData.XFieldComparatorSource {

    private final IndexGeoPointFieldData<?> indexFieldData;
    private final double lat;
    private final double lon;
    private final DistanceUnit unit;
    private final GeoDistance geoDistance;
    private final SortMode sortMode;

    public GeoDistanceComparatorSource(IndexGeoPointFieldData<?> indexFieldData, double lat, double lon, DistanceUnit unit, GeoDistance geoDistance, SortMode sortMode) {
        this.indexFieldData = indexFieldData;
        this.lat = lat;
        this.lon = lon;
        this.unit = unit;
        this.geoDistance = geoDistance;
        this.sortMode = sortMode;
    }

    @Override
    public SortField.Type reducedType() {
        return SortField.Type.DOUBLE;
    }

    @Override
    public FieldComparator<?> newComparator(String fieldname, int numHits, int sortPos, boolean reversed) throws IOException {
        assert indexFieldData.getFieldNames().indexName().equals(fieldname);
        return new GeoDistanceComparator(numHits, indexFieldData, lat, lon, unit, geoDistance, sortMode);
    }
}
