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

import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MetadataFieldMapper;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A registry for all field mappers.
 */
public final class MapperRegistry {

    private final Map<String, Mapper.TypeParser> mapperParsers;
    private final Map<String, MetadataFieldMapper.TypeParser> metadataMapperParsers;

    public MapperRegistry(Map<String, Mapper.TypeParser> mapperParsers,
            Map<String, MetadataFieldMapper.TypeParser> metadataMapperParsers) {
        this.mapperParsers = Collections.unmodifiableMap(new LinkedHashMap<>(mapperParsers));
        this.metadataMapperParsers = Collections.unmodifiableMap(new LinkedHashMap<>(metadataMapperParsers));
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
    public Map<String, MetadataFieldMapper.TypeParser> getMetadataMapperParsers() {
        return metadataMapperParsers;
    }
}
