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

package org.elasticsearch.index.mapper;

import org.elasticsearch.index.mapper.MetadataFieldMapper.TypeParser;
import org.elasticsearch.index.query.RankFeatureQueryBuilder;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class MapperExtrasPlugin extends Plugin implements MapperPlugin, SearchPlugin {

    @Override
    public Map<String, Mapper.TypeParser> getMappers() {
        Map<String, Mapper.TypeParser> mappers = new LinkedHashMap<>();
        mappers.put(ScaledFloatFieldMapper.CONTENT_TYPE, new ScaledFloatFieldMapper.TypeParser());
        mappers.put(TokenCountFieldMapper.CONTENT_TYPE, new TokenCountFieldMapper.TypeParser());
        mappers.put(RankFeatureFieldMapper.CONTENT_TYPE, new RankFeatureFieldMapper.TypeParser());
        mappers.put(RankFeaturesFieldMapper.CONTENT_TYPE, new RankFeaturesFieldMapper.TypeParser());
        mappers.put(SearchAsYouTypeFieldMapper.CONTENT_TYPE, new SearchAsYouTypeFieldMapper.TypeParser());
        return Collections.unmodifiableMap(mappers);
    }

    @Override
    public Map<String, TypeParser> getMetadataMappers() {
        return Collections.singletonMap(RankFeatureMetaFieldMapper.CONTENT_TYPE, new RankFeatureMetaFieldMapper.TypeParser());
    }

    @Override
    public List<QuerySpec<?>> getQueries() {
        return Collections.singletonList(
            new QuerySpec<>(RankFeatureQueryBuilder.NAME, RankFeatureQueryBuilder::new,
                p -> RankFeatureQueryBuilder.PARSER.parse(p, null)));
    }

}
