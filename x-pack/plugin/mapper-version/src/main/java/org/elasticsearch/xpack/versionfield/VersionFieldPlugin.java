/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.versionfield;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.plugins.ExtensiblePlugin;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.DocValueFormat;

import java.util.List;
import java.util.Map;

/**
 * Implements {@link ExtensiblePlugin} so that other plugins (e.g. x-pack-ql and x-pack-esql-core) may declare it in
 * their {@code extendedPlugins} and load the {@link Version} class from this module at runtime instead of bundling a
 * duplicate copy, which would cause a jar-hell conflict when several such plugins are loaded together.
 */
public class VersionFieldPlugin extends Plugin implements MapperPlugin, ExtensiblePlugin {

    public VersionFieldPlugin(Settings settings) {}

    @Override
    public Map<String, Mapper.TypeParser> getMappers() {
        return Map.of(VersionStringFieldMapper.CONTENT_TYPE, VersionStringFieldMapper.PARSER);
    }

    @Override
    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return List.of(
            new NamedWriteableRegistry.Entry(
                DocValueFormat.class,
                VersionStringFieldMapper.VERSION_DOCVALUE.getWriteableName(),
                in -> VersionStringFieldMapper.VERSION_DOCVALUE
            )
        );
    }
}
