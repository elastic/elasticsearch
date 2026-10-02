/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.analytics.mapper;

import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;

import java.util.Map;

class TestAnalyticsPlugin extends Plugin implements SearchPlugin, ActionPlugin, MapperPlugin {

    TestAnalyticsPlugin() {}

    @Override
    public Map<String, Mapper.TypeParser> getMappers() {
        return Map.of(TDigestFieldMapper.CONTENT_TYPE, TDigestFieldMapper.PARSER);
    }
}
