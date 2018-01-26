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

package org.elasticsearch.plugins;

import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MetadataFieldMapper;

import java.util.Collections;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * An extension point for {@link Plugin} implementations to add custom mappers
 */
public interface MapperPlugin {

    /**
     * Returns additional mapper implementations added by this plugin.
     * <p>
     * The key of the returned {@link Map} is the unique name for the mapper which will be used
     * as the mapping {@code type}, and the value is a {@link Mapper.TypeParser} to parse the
     * mapper settings into a {@link Mapper}.
     */
    default Map<String, Mapper.TypeParser> getMappers() {
        return Collections.emptyMap();
    }

    /**
     * Returns additional metadata mapper implementations added by this plugin.
     * <p>
     * The key of the returned {@link Map} is the unique name for the metadata mapper, which
     * is used in the mapping json to configure the metadata mapper, and the value is a
     * {@link MetadataFieldMapper.TypeParser} to parse the mapper settings into a
     * {@link MetadataFieldMapper}.
     */
    default Map<String, MetadataFieldMapper.TypeParser> getMetadataMappers() {
        return Collections.emptyMap();
    }

    /**
     * Returns a function that given an index name returns a predicate which fields must match in order to be returned by get mappings,
     * get index, get field mappings and field capabilities API. Useful to filter the fields that such API return. The predicate receives
     * the field name as input argument and should return true to show the field and false to hide it.
     */
    default Function<String, Predicate<String>> getFieldFilter() {
        return NOOP_FIELD_FILTER;
    }

    /**
     * The default field predicate applied, which doesn't filter anything. That means that by default get mappings, get index
     * get field mappings and field capabilities API will return every field that's present in the mappings.
     */
    Predicate<String> NOOP_FIELD_PREDICATE = field -> true;

    /**
     * The default field filter applied, which doesn't filter anything. That means that by default get mappings, get index
     * get field mappings and field capabilities API will return every field that's present in the mappings.
     */
    Function<String, Predicate<String>> NOOP_FIELD_FILTER = index -> NOOP_FIELD_PREDICATE;
}
