/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.countedkeyword;

import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class CountedKeywordMapperPlugin extends Plugin implements MapperPlugin, SearchPlugin {
    @Override
    public Map<String, Mapper.TypeParser> getMappers() {
        Map<String, Mapper.TypeParser> mappers = new LinkedHashMap<>();
        mappers.put(CountedKeywordFieldMapper.CONTENT_TYPE, CountedKeywordFieldMapper.PARSER);
        return Collections.unmodifiableMap(mappers);
    }

    @Override
    public List<SearchPlugin.AggregationSpec> getAggregations() {
        List<SearchPlugin.AggregationSpec> specs = new ArrayList<>();
        specs.add(
            new SearchPlugin.AggregationSpec(
                CountedTermsAggregationBuilder.NAME,
                CountedTermsAggregationBuilder::new,
                CountedTermsAggregationBuilder.PARSER
            ).setAggregatorRegistrar(CountedTermsAggregationBuilder::registerAggregators)
        );
        return List.copyOf(specs);
    }
}
