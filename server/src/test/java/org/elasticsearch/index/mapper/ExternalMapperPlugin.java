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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MetadataFieldMapper;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.Plugin;

public class ExternalMapperPlugin extends Plugin implements MapperPlugin {

    public static final String EXTERNAL = "external";
    public static final String EXTERNAL_BIS = "external_bis";
    public static final String EXTERNAL_UPPER = "external_upper";

    @Override
    public Map<String, Mapper.TypeParser> getMappers() {
        Map<String, Mapper.TypeParser> mappers = new HashMap<>();
        mappers.put(EXTERNAL, new ExternalMapper.TypeParser(EXTERNAL, "foo"));
        mappers.put(EXTERNAL_BIS, new ExternalMapper.TypeParser(EXTERNAL_BIS, "bar"));
        mappers.put(EXTERNAL_UPPER, new ExternalMapper.TypeParser(EXTERNAL_UPPER, "FOO BAR"));
        mappers.put(FakeStringFieldMapper.CONTENT_TYPE, new FakeStringFieldMapper.TypeParser());
        return Collections.unmodifiableMap(mappers);
    }

    @Override
    public Map<String, MetadataFieldMapper.TypeParser> getMetadataMappers() {
        return Collections.singletonMap(ExternalMetadataMapper.CONTENT_TYPE, new ExternalMetadataMapper.TypeParser());
    }

}
