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

// Implements ExtensiblePlugin (a no-op marker here, using the default loadExtensions) so other modules may
// declare mapper-version in their extendedPlugins and share its single class identity (e.g. x-pack-esql-core
// and x-pack-ql, which use the Version type). Without this, extending mapper-version fails at node startup
// with "cannot extend non-extensible plugin".
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
