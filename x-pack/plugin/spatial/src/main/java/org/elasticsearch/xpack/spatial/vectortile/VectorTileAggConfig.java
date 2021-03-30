/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.spatial.vectortile;


import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.Map;

import static java.util.Collections.emptyMap;


public class VectorTileAggConfig {

    private static final ParseField SCALING = new ParseField("scaling");

    public static final ObjectParser<VectorTileAggConfig, Void> PARSER =
        new ObjectParser<>("gridmvt_config", VectorTileAggConfig::new);

    static {
        PARSER.declareField(
            VectorTileAggConfig::setRuntimeField,
            XContentParser::map,
            SearchSourceBuilder.RUNTIME_MAPPINGS_FIELD,
            ObjectParser.ValueType.OBJECT
        );
        PARSER.declareField(
            VectorTileAggConfig::setQueryBuilder,
            (CheckedFunction<XContentParser, QueryBuilder, IOException>) AbstractQueryBuilder::parseInnerQueryBuilder,
            SearchSourceBuilder.QUERY_FIELD,
            ObjectParser.ValueType.OBJECT
        );
        PARSER.declareField(
            VectorTileAggConfig::setAggBuilder,
            AggregatorFactories::parseAggregators,
            SearchSourceBuilder.AGGS_FIELD,
            ObjectParser.ValueType.OBJECT
        );
        PARSER.declareInt(VectorTileAggConfig::setScaling, SCALING);
    }

    public static VectorTileAggConfig getInstance() {
        return new VectorTileAggConfig();
    }

    private QueryBuilder queryBuilder;
    private AggregatorFactories.Builder aggBuilder;
    private int scaling = 8;
    private Map<String, Object> runtimeMappings = emptyMap();

    private VectorTileAggConfig() {
    }

    private void setQueryBuilder(QueryBuilder queryBuilder) {
        this.queryBuilder = queryBuilder;
    }

    public QueryBuilder getQueryBuilder() {
        return queryBuilder;
    }

    private void setAggBuilder(AggregatorFactories.Builder aggBuilder) {
        this.aggBuilder = aggBuilder;
    }

    public AggregatorFactories.Builder getAggBuilder() {
        return aggBuilder;
    }

    private void setScaling(int scaling) {
        this.scaling = scaling;
    }

    public int getScaling() {
        return scaling;
    }

    private void setRuntimeField(Map<String, Object> runtimeMappings) {
        this.runtimeMappings = runtimeMappings;
    }

    public Map<String, Object> getRuntimeMappings() {
        return runtimeMappings;
    }
}
