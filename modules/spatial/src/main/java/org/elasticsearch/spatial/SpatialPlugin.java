/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
package org.elasticsearch.spatial;

import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.plugins.ExtensiblePlugin;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.spatial.index.mapper.AbstractGeometryFieldMapper;
import org.elasticsearch.spatial.index.mapper.GeoPointFieldMapper;
import org.elasticsearch.spatial.index.mapper.GeoShapeFieldMapper;
import org.elasticsearch.spatial.index.query.GeoBoundingBoxQueryBuilder;
import org.elasticsearch.spatial.index.query.GeoDistanceQueryBuilder;
import org.elasticsearch.spatial.index.query.GeoPolygonQueryBuilder;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class SpatialPlugin extends Plugin implements MapperPlugin, SearchPlugin, ExtensiblePlugin {
    @Override
    public Map<String, Mapper.TypeParser> getMappers() {
        Map<String, Mapper.TypeParser> mappers = new LinkedHashMap<>();
        mappers.put(GeoPointFieldMapper.CONTENT_TYPE, new GeoPointFieldMapper.TypeParser());
        mappers.put(GeoShapeFieldMapper.CONTENT_TYPE, new AbstractGeometryFieldMapper.TypeParser());
        return Collections.unmodifiableMap(mappers);
    }

    @Override
    public List<QuerySpec<?>> getQueries() {
        return Arrays.asList(
            new QuerySpec<>(GeoBoundingBoxQueryBuilder.NAME, GeoBoundingBoxQueryBuilder::new, GeoBoundingBoxQueryBuilder::fromXContent),
            new QuerySpec<>(GeoDistanceQueryBuilder.NAME, GeoDistanceQueryBuilder::new, GeoDistanceQueryBuilder::fromXContent),
            new QuerySpec<>(GeoPolygonQueryBuilder.NAME, GeoPolygonQueryBuilder::new, GeoPolygonQueryBuilder::fromXContent));
    }
}
