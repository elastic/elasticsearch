/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.test.integration.indices.source;

import org.elasticsearch.common.BytesHolder;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.assistedinject.Assisted;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.source.ParsingExternalSourceProvider;

import java.io.IOException;
import java.util.Map;

import static java.util.Collections.emptyMap;

/**
 * Emulates a source provider that can rehydrate source based on type and id
 */
public class IdBasedExternalSourceProvider extends ParsingExternalSourceProvider {

    public static class Defaults {
        public static String NAME = "idbased";
        public static String SOURCE_PATTERN = "{\"_id\":\"%2$s\", \"_type\":\"%1$s\", \"body\":\"This record has id %2$s and type %1$s\"}";
    }

    private final String sourcePattern;

    @Inject
    public IdBasedExternalSourceProvider(@Assisted String name, @Assisted Settings settings) {
        super(name);
        this.sourcePattern = settings.get("source_pattern", Defaults.SOURCE_PATTERN);
    }

    @Override
    public Map<String, Object> dehydrateSource(String type, String id, Map<String, Object> source) throws IOException {
        return emptyMap();
    }

    @Override
    public BytesHolder rehydrateSource(String type, String id, byte[] source, int sourceOffset, int sourceLength) {
        byte[] buffer = String.format(sourcePattern, type, id).getBytes();
        return new BytesHolder(buffer);
    }

}
