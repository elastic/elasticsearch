/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper.extras;

import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MetadataFieldMapper.TypeParser;
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
        mappers.put(ScaledFloatFieldMapper.CONTENT_TYPE, ScaledFloatFieldMapper.PARSER);
        mappers.put(TokenCountFieldMapper.CONTENT_TYPE, TokenCountFieldMapper.PARSER);
        mappers.put(RankFeatureFieldMapper.CONTENT_TYPE, RankFeatureFieldMapper.PARSER);
        mappers.put(RankFeaturesFieldMapper.CONTENT_TYPE, RankFeaturesFieldMapper.PARSER);
        mappers.put(SearchAsYouTypeFieldMapper.CONTENT_TYPE, SearchAsYouTypeFieldMapper.PARSER);
        mappers.put(MatchOnlyTextFieldMapper.CONTENT_TYPE, MatchOnlyTextFieldMapper.PARSER);
        return Collections.unmodifiableMap(mappers);
    }

    @Override
    public Map<String, TypeParser> getMetadataMappers() {
        return Collections.singletonMap(RankFeatureMetaFieldMapper.CONTENT_TYPE, RankFeatureMetaFieldMapper.PARSER);
    }

    @Override
    public List<QuerySpec<?>> getQueries() {
        return Collections.singletonList(
            new QuerySpec<>(RankFeatureQueryBuilder.NAME, RankFeatureQueryBuilder::new, p -> RankFeatureQueryBuilder.PARSER.parse(p, null))
        );
    }

}
