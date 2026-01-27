/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugin.lance;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.plugin.lance.mapper.LanceVectorFieldMapper;
import org.elasticsearch.plugin.lance.query.LanceKnnQueryBuilder;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.xcontent.ParseField;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LanceVectorPlugin extends Plugin implements MapperPlugin, SearchPlugin {

    /**
     * Setting to enable Lance profiling for detailed timing information.
     * <p>
     * When enabled, Lance kNN searches will collect detailed timing information
     * for each stage of the search execution pipeline. This timing data is returned
     * in the profile section of the search response when profile=true is set.
     * <p>
     * Default is disabled to minimize overhead in production environments.
     * Profiling adds minimal overhead when disabled (&lt; 1%).
     */
    public static final Setting<Boolean> LANCE_PROFILING_ENABLED = Setting.boolSetting(
        "lance.profiling.enabled",
        false,
        Setting.Property.NodeScope
    );

    private final boolean profilingEnabled;

    public LanceVectorPlugin(Settings settings) {
        this.profilingEnabled = LANCE_PROFILING_ENABLED.get(settings);
    }

    @Override
    public List<Setting<?>> getSettings() {
        List<Setting<?>> settings = new ArrayList<>();
        settings.add(LANCE_PROFILING_ENABLED);
        return settings;
    }

    /**
     * Check if Lance profiling is enabled.
     *
     * @return true if profiling is enabled, false otherwise
     */
    public boolean isProfilingEnabled() {
        return profilingEnabled;
    }

    @Override
    public Map<String, Mapper.TypeParser> getMappers() {
        Map<String, Mapper.TypeParser> mappers = new HashMap<>();
        mappers.put(LanceVectorFieldMapper.CONTENT_TYPE, LanceVectorFieldMapper.PARSER);
        return mappers;
    }

    @Override
    public List<QuerySpec<?>> getQueries() {
        return List.of(
            new QuerySpec<>(
                new ParseField(LanceKnnQueryBuilder.NAME),
                LanceKnnQueryBuilder::new,  // Reader from StreamInput
                LanceKnnQueryBuilder::fromXContent  // Parser from XContentParser
            )
        );
    }
}
