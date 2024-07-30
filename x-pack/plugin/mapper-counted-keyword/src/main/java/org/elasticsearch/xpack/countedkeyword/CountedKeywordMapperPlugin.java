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
import org.elasticsearch.search.aggregations.bucket.countedterms.CountedTermsAggregationBuilder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * <p>This plugin adds two associated features:</p>
 * <ol>
 *    <li>The mapping type <code>counted_keyword</code> that behaves like <code>keyword</code> except that it counts duplicate values.</li>
 *    <li>The <code>counted_terms</code> aggregation that operates on fields mapped as <code>counted_keyword</code> and considers
 *    duplicate values in the <code>doc_count</code> that it returns.</li>
 * </ol>
 *
 * <p>Both features are considered a tech preview and are thus intentionally undocumented.</p>
 */
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
