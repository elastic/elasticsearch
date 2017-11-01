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

package org.elasticsearch.legacygeo;

import org.elasticsearch.common.geo.ShapesAvailability;
import org.elasticsearch.common.geo.builders.ShapeBuilders;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.index.mapper.GeoShapeFieldMapper;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.query.GeoShapeQueryBuilder;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class LegacyGeoPlugin extends Plugin implements MapperPlugin, SearchPlugin {

    @Override
    public Map<String, Mapper.TypeParser> getMappers() {
        // No leniency needed since the transport client does not register mappers
        return Collections.singletonMap(GeoShapeFieldMapper.CONTENT_TYPE, new GeoShapeFieldMapper.TypeParser());
    }

    @Override
    public List<QuerySpec<?>> getQueries() {
        // No leniency needed since the transport client does not register queries
        return Collections.singletonList(
                new QuerySpec<>(GeoShapeQueryBuilder.NAME, GeoShapeQueryBuilder::new, GeoShapeQueryBuilder::fromXContent));
    }

    @Override
    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        List<NamedWriteableRegistry.Entry> namedWriteables = new ArrayList<>();
        if (ShapesAvailability.JTS_AVAILABLE && ShapesAvailability.SPATIAL4J_AVAILABLE) {
            // We need this leniency for the transport client
            ShapeBuilders.register(namedWriteables);
        }
        return namedWriteables;
    }
}
