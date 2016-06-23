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

import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.plugins.Plugin;

public class ExternalMapperPlugin extends Plugin {

    public static final String EXTERNAL = "external";
    public static final String EXTERNAL_BIS = "external_bis";
    public static final String EXTERNAL_UPPER = "external_upper";

    public void onModule(IndicesModule indicesModule) {
        indicesModule.registerMetadataMapper(ExternalMetadataMapper.CONTENT_TYPE, new ExternalMetadataMapper.TypeParser());
        indicesModule.registerMapper(EXTERNAL, new ExternalMapper.TypeParser(EXTERNAL, "foo"));
        indicesModule.registerMapper(EXTERNAL_BIS, new ExternalMapper.TypeParser(EXTERNAL_BIS, "bar"));
        indicesModule.registerMapper(EXTERNAL_UPPER, new ExternalMapper.TypeParser(EXTERNAL_UPPER, "FOO BAR"));
        indicesModule.registerMapper(FakeStringFieldMapper.CONTENT_TYPE, new FakeStringFieldMapper.TypeParser());
    }

}
