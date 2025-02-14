/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.plugins.FieldPredicate;
import org.elasticsearch.plugins.MapperPlugin;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * A registry for all field mappers.
 */
public final class MapperRegistry {

    private final Map<String, Mapper.TypeParser> mapperParsers;
    private final Map<String, RuntimeField.Parser> runtimeFieldParsers;
    private final Map<String, MetadataFieldMapper.TypeParser> metadataMapperParsers;
    private final Map<String, MetadataFieldMapper.TypeParser> metadataMapperParsers7x;
    private final Map<String, MetadataFieldMapper.TypeParser> metadataMapperParsers6x;
    private final Map<String, MetadataFieldMapper.TypeParser> metadataMapperParsers5x;
    private final Function<String, FieldPredicate> fieldFilter;

    public MapperRegistry(
        Map<String, Mapper.TypeParser> mapperParsers,
        Map<String, RuntimeField.Parser> runtimeFieldParsers,
        Map<String, MetadataFieldMapper.TypeParser> metadataMapperParsers,
        Function<String, FieldPredicate> fieldFilter
    ) {
        this.mapperParsers = Collections.unmodifiableMap(new LinkedHashMap<>(mapperParsers));
        this.runtimeFieldParsers = runtimeFieldParsers;
        this.metadataMapperParsers = Collections.unmodifiableMap(new LinkedHashMap<>(metadataMapperParsers));
        Map<String, MetadataFieldMapper.TypeParser> metadata7x = new LinkedHashMap<>(metadataMapperParsers);
        metadata7x.remove(NestedPathFieldMapper.NAME);
        this.metadataMapperParsers7x = metadata7x;
        Map<String, MetadataFieldMapper.TypeParser> metadata6x = new LinkedHashMap<>(metadata7x);
        this.metadataMapperParsers6x = metadata6x;
        Map<String, MetadataFieldMapper.TypeParser> metadata5x = new LinkedHashMap<>(metadata7x);
        metadata5x.put(LegacyTypeFieldMapper.NAME, LegacyTypeFieldMapper.PARSER);
        this.metadataMapperParsers5x = metadata5x;
        this.fieldFilter = fieldFilter;
    }

    /**
     * Return a mapper parser for the given type and index creation version.
     */
    public Mapper.TypeParser getMapperParser(String type, IndexVersion indexVersionCreated) {
        Mapper.TypeParser parser = mapperParsers.get(type);
        if (indexVersionCreated.isLegacyIndexVersion()) {
            if (parser == null || parser.supportsVersion(indexVersionCreated) == false) {
                return PlaceHolderFieldMapper.PARSER.apply(type);
            }
            return parser;
        } else {
            assert parser == null || parser.supportsVersion(indexVersionCreated);
            return parser;
        }
    }

    public Map<String, RuntimeField.Parser> getRuntimeFieldParsers() {
        return runtimeFieldParsers;
    }

    /**
     * Return a map of the meta mappers that have been registered. The
     * returned map uses the name of the field as a key.
     */
    public Map<String, MetadataFieldMapper.TypeParser> getMetadataMapperParsers(IndexVersion indexCreatedVersion) {
        if (indexCreatedVersion.onOrAfter(IndexVersions.V_8_0_0)) {
            return metadataMapperParsers;
        } else if (indexCreatedVersion.onOrAfter(IndexVersions.V_7_0_0)) {
            return metadataMapperParsers7x;
        } else if (indexCreatedVersion.onOrAfter(IndexVersion.fromId(6000099))) {
            return metadataMapperParsers6x;
        } else if (indexCreatedVersion.onOrAfter(IndexVersion.fromId(5000099))) {
            return metadataMapperParsers5x;
        } else {
            throw new AssertionError("unknown version: " + indexCreatedVersion);
        }
    }

    /**
     * Returns a function that given an index name, returns a predicate that fields must match in order to be returned by get mappings,
     * get index, get field mappings and field capabilities API. Useful to filter the fields that such API return.
     * The predicate receives the field name as input arguments. In case multiple plugins register a field filter through
     * {@link MapperPlugin#getFieldFilter()}, only fields that match all the registered filters will be returned by get mappings,
     * get index, get field mappings and field capabilities API.
     */
    public Function<String, FieldPredicate> getFieldFilter() {
        return fieldFilter;
    }
}
