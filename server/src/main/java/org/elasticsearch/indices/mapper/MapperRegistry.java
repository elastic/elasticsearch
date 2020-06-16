/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.indices.mapper;

import org.elasticsearch.Version;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MetadataFieldMapper;
import org.elasticsearch.index.mapper.NestedPathFieldMapper;
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
    private final Map<String, MetadataFieldMapper.TypeParser> metadataMapperParsers;
    private final Map<String, MetadataFieldMapper.TypeParser> metadataMapperParsers7x;
    private final Function<String, Predicate<String>> fieldFilter;


    public MapperRegistry(Map<String, Mapper.TypeParser> mapperParsers,
            Map<String, MetadataFieldMapper.TypeParser> metadataMapperParsers, Function<String, Predicate<String>> fieldFilter) {
        this.mapperParsers = Collections.unmodifiableMap(new LinkedHashMap<>(mapperParsers));
        this.metadataMapperParsers = Collections.unmodifiableMap(new LinkedHashMap<>(metadataMapperParsers));
        Map<String, MetadataFieldMapper.TypeParser> metadata7x = new LinkedHashMap<>(metadataMapperParsers);
        metadata7x.remove(NestedPathFieldMapper.NAME);
        this.metadataMapperParsers7x = metadata7x;
        this.fieldFilter = fieldFilter;
    }

    /**
     * Return a map of the mappers that have been registered. The
     * returned map uses the type of the field as a key.
     */
    public Map<String, Mapper.TypeParser> getMapperParsers() {
        return mapperParsers;
    }

    /**
     * Return a map of the meta mappers that have been registered. The
     * returned map uses the name of the field as a key.
     */
    public Map<String, MetadataFieldMapper.TypeParser> getMetadataMapperParsers(Version indexCreatedVersion) {
        if (indexCreatedVersion.onOrAfter(Version.V_8_0_0)) {
            return metadataMapperParsers;
        }
        return metadataMapperParsers7x;
    }

    /**
     * Returns true if the provided field is a registered metadata field, false otherwise
     */
    public boolean isMetadataField(Version indexCreatedVersion, String field) {
        return getMetadataMapperParsers(indexCreatedVersion).containsKey(field);
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
