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
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.DocValueFormat;

import java.util.List;
import java.util.Map;

/**
 * Plugin for version field mapping in Elasticsearch.
 * <p>
 * This plugin provides the {@code version} field type, which is optimized for storing
 * and querying software version strings (e.g., "1.2.3", "2.0.0-beta1"). The field type
 * uses a specialized encoding that enables efficient sorting and range queries on version
 * values while understanding semantic versioning conventions.
 * </p>
 * <p><b>Usage Example:</b></p>
 * <pre>{@code
 * PUT /my-index
 * {
 *   "mappings": {
 *     "properties": {
 *       "software_version": {
 *         "type": "version"
 *       }
 *     }
 *   }
 * }
 * }</pre>
 */
public class VersionFieldPlugin extends Plugin implements MapperPlugin {

    /**
     * Constructs a new VersionFieldPlugin with the specified settings.
     *
     * @param settings the node settings (not used by this plugin)
     */
    public VersionFieldPlugin(Settings settings) {}

    /**
     * Returns the field mappers provided by this plugin.
     *
     * @return a map containing the version field type parser
     */
    @Override
    public Map<String, Mapper.TypeParser> getMappers() {
        return Map.of(VersionStringFieldMapper.CONTENT_TYPE, VersionStringFieldMapper.PARSER);
    }

    /**
     * Returns the named writeables provided by this plugin.
     * <p>
     * Registers the version-specific doc value format for serialization
     * of version field values across the cluster.
     * </p>
     *
     * @return a list containing the version doc value format entry
     */
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
