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

package org.elasticsearch.index.mapper.externalvalues;

import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.plugins.Plugin;

import java.io.Closeable;
import java.util.List;

public class ExternalMapperPlugin extends Plugin {

    public static final String EXTERNAL = "external";
    public static final String EXTERNAL_BIS = "external_bis";
    public static final String EXTERNAL_UPPER = "external_upper";

    @Override
    public String name() {
        return "external-mappers";
    }

    @Override
    public String description() {
        return "External Mappers Plugin";
    }

    @Override
    public void onIndexService(IndexService indexService) {
        final MapperService mapperService = indexService.mapperService();
        mapperService.documentMapperParser().putRootTypeParser(ExternalMetadataMapper.CONTENT_TYPE, new ExternalMetadataMapper.TypeParser());
        mapperService.documentMapperParser().putTypeParser(EXTERNAL, new ExternalMapper.TypeParser(EXTERNAL, "foo"));
        mapperService.documentMapperParser().putTypeParser(EXTERNAL_BIS, new ExternalMapper.TypeParser(EXTERNAL_BIS, "bar"));
        mapperService.documentMapperParser().putTypeParser(EXTERNAL_UPPER, new ExternalMapper.TypeParser(EXTERNAL_UPPER, "FOO BAR"));
    }
}