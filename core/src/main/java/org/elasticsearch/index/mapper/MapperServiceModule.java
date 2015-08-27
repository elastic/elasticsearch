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

package org.elasticsearch.index.mapper;

import com.google.common.collect.ImmutableSet;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.util.ExtensionPoint;
import org.elasticsearch.index.mapper.internal.AllFieldMapper;
import org.elasticsearch.index.mapper.internal.FieldNamesFieldMapper;
import org.elasticsearch.index.mapper.internal.IdFieldMapper;
import org.elasticsearch.index.mapper.internal.IndexFieldMapper;
import org.elasticsearch.index.mapper.internal.ParentFieldMapper;
import org.elasticsearch.index.mapper.internal.RoutingFieldMapper;
import org.elasticsearch.index.mapper.internal.SourceFieldMapper;
import org.elasticsearch.index.mapper.internal.TTLFieldMapper;
import org.elasticsearch.index.mapper.internal.TimestampFieldMapper;
import org.elasticsearch.index.mapper.internal.TypeFieldMapper;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.index.mapper.internal.VersionFieldMapper;

import java.util.Set;

/**
 * The infrastructure around DocumentMappers and FieldMappers.
 */
public class MapperServiceModule extends AbstractModule {
    // TODO it should be possible to move all of the rootTypeParsers into rootParsers as "extensions" registered in Elasticsearch core.
    private static final Set<String> ROOT_TYPE_PARSERS = ImmutableSet.of(IndexFieldMapper.NAME, SourceFieldMapper.NAME,
            TypeFieldMapper.NAME, AllFieldMapper.NAME, ParentFieldMapper.NAME, RoutingFieldMapper.NAME, TimestampFieldMapper.NAME,
            TTLFieldMapper.NAME, UidFieldMapper.NAME, VersionFieldMapper.NAME, IdFieldMapper.NAME, FieldNamesFieldMapper.NAME);
    private final ExtensionPoint.ClassMap<DocumentMapperRootParser> rootParsers = new ExtensionPoint.ClassMap<>("mapping_root_parser",
            DocumentMapperRootParser.class, ROOT_TYPE_PARSERS);

    @Override
    protected void configure() {
        bind(MapperService.class).asEagerSingleton();
        rootParsers.bind(binder());
    }

    /**
     * Configure the parser for a root field in the mapping. Like _ttl or _source.
     * @param key the key of the root element to parse
     * @param clazz the type to do the parsing
     */
    public void addRootParser(String key, Class<? extends DocumentMapperRootParser> clazz) {
        rootParsers.registerExtension(key, clazz);
    }
}
