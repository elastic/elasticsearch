/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.aggregations.support.geopoints;

import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.index.fielddata.GeoPointValues;
import org.elasticsearch.search.aggregations.support.FieldDataSource;
import org.elasticsearch.search.aggregations.support.ValuesSource;

/**
 * A source of geo points.
 */
public final class GeoPointValuesSource implements ValuesSource {

    private final FieldDataSource.GeoPoint source;

    public GeoPointValuesSource(FieldDataSource.GeoPoint source) {
        this.source = source;
    }

    @Override
    public FieldDataSource.MetaData metaData() {
        return source.metaData();
    }

    @Override
    public BytesValues bytesValues() {
        return source.bytesValues();
    }

    public final GeoPointValues values() {
        return source.geoPointValues();
    }

}
