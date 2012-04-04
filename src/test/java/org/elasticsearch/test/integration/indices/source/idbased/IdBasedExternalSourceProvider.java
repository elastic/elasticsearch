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

package org.elasticsearch.test.integration.indices.source.idbased;

import org.elasticsearch.common.BytesHolder;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.source.ExternalSourceProvider;

import java.io.IOException;

/**
 *
 */
public class IdBasedExternalSourceProvider implements ExternalSourceProvider {

    public static class Defaults {
        public static String SOURCE_PATTERN = "{\"_id\":\"%2$s\", \"_type\":\"%1$s\", \"body\":\"This record has id %2$s and type %1$s\"}";
    }

    private final String sourcePattern;

    @Inject
    public IdBasedExternalSourceProvider(@IndexSettings Settings settings) {
        this.sourcePattern = settings.get("index.source.provider.id_based.source_pattern", Defaults.SOURCE_PATTERN);
    }

    @Override
    public BytesHolder dehydrateSource(String type, String id, byte[] source, int sourceOffset, int sourceLength) throws IOException {
        // We don't need to store source at all
        return null;
    }

    @Override
    public BytesHolder rehydrateSource(String type, String id, byte[] source, int sourceOffset, int sourceLength) {
        byte[] buffer = String.format(sourcePattern, type, id).getBytes();
        return new BytesHolder(buffer);
    }

    @Override
    public boolean enabled() {
        return true;
    }
}
