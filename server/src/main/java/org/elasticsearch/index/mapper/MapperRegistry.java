/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.Version;
import org.elasticsearch.plugins.MapperPlugin;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * A registry for all field mappers.
 */
public final class MapperRegistry {

    private final Map<String, Mapper.TypeParser> mapperParsers;
    private final Map<String, RuntimeField.Parser> runtimeFieldParsers;
    private final Map<String, MetadataFieldMapper.TypeParser> metadataMapperParsers;
    private final Map<String, MetadataFieldMapper.TypeParser> metadataMapperParsers6x;
    private final Function<String, Predicate<String>> fieldFilter;

    public MapperRegistry(
        Map<String, Mapper.TypeParser> mapperParsers,
        Map<String, RuntimeField.Parser> runtimeFieldParsers,
        Map<String, MetadataFieldMapper.TypeParser> metadataMapperParsers,
        Function<String, Predicate<String>> fieldFilter
    ) {
        this.mapperParsers = Collections.unmodifiableMap(new LinkedHashMap<>(mapperParsers));
        this.runtimeFieldParsers = runtimeFieldParsers;
        this.metadataMapperParsers = Collections.unmodifiableMap(new LinkedHashMap<>(metadataMapperParsers));
        // add the _all field mapper for indices created in 6x
        Map<String, MetadataFieldMapper.TypeParser> metadata6x = new LinkedHashMap<>();
        metadata6x.put(AllFieldMapper.NAME, AllFieldMapper.PARSER);
        metadata6x.putAll(metadataMapperParsers);
        this.metadataMapperParsers6x = Collections.unmodifiableMap(metadata6x);
        this.fieldFilter = fieldFilter;
    }

    /**
     * Return a map of the mappers that have been registered. The
     * returned map uses the type of the field as a key.
     */
    public Map<String, Mapper.TypeParser> getMapperParsers() {
        return mapperParsers;
    }

    public Map<String, RuntimeField.Parser> getRuntimeFieldParsers() {
        return runtimeFieldParsers;
    }

    /**
     * Return a map of the meta mappers that have been registered. The
     * returned map uses the name of the field as a key.
     */
    public Map<String, MetadataFieldMapper.TypeParser> getMetadataMapperParsers(Version indexCreatedVersion) {
        return indexCreatedVersion.onOrAfter(Version.V_7_0_0) ? metadataMapperParsers : metadataMapperParsers6x;
    }

    /**
     * Return a map of all meta mappers that have been registered in all compatible versions.
     */
    public Map<String, MetadataFieldMapper.TypeParser> getAllMetadataMapperParsers() {
        return metadataMapperParsers;
    }

    /**
     * Returns a function that given an index name, returns a predicate that fields must match in order to be returned by get mappings,
     * get index, get field mappings and field capabilities API. Useful to filter the fields that such API return.
     * The predicate receives the field name as input arguments. In case multiple plugins register a field filter through
     * {@link MapperPlugin#getFieldFilter()}, only fields that match all the registered filters will be returned by get mappings,
     * get index, get field mappings and field capabilities API.
     */
    public Function<String, Predicate<String>> getFieldFilter() {
        return fieldFilter;
    }
}
