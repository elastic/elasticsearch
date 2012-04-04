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

package org.elasticsearch.test.integration.indices.source.transform;

import com.google.common.collect.Maps;
import org.elasticsearch.common.BytesHolder;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.CachedStreamOutput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.source.ExternalSourceProvider;

import java.io.IOException;
import java.util.Map;

import static java.util.Collections.emptyMap;

/**
 *
 */
public class TransformingExternalSourceProvider implements ExternalSourceProvider {
    public static class Defaults {
        public static String SOURCE_PATTERN = "{\"_id\":\"%1$s\", \"_type\":\"%2$s\", \"body\":\"Content of a file %3$s\", " +
                "\"file\":\"%3$s\", \"generated\":true}";
        public static String PATH_FIELD = "file";
    }

    private final String sourcePattern;

    private final String pathField;

    @Inject
    public TransformingExternalSourceProvider(@IndexSettings Settings settings) {
        this.sourcePattern = settings.get("source_pattern", Defaults.SOURCE_PATTERN);
        this.pathField = settings.get("path_field", Defaults.PATH_FIELD);
    }

    public Map<String, Object> dehydrateSource(Map<String, Object> source) throws IOException {
        Object pathFieldObject = source.get(pathField);
        if (pathFieldObject != null && pathFieldObject instanceof String) {
            // Replace path with just the portion that is needed to restore source in the future
            Map<String, Object> dehydratedMap = Maps.newHashMap();
            dehydratedMap.put(pathField, pathFieldObject);
            return dehydratedMap;
        } else {
            // Path field is not found - don't store source at all
            return emptyMap();
        }
    }

    @Override
    public BytesHolder dehydrateSource(String type, String id, byte[] source, int sourceOffset, int sourceLength) throws IOException {
        Tuple<XContentType, Map<String, Object>> mapTuple = XContentHelper.convertToMap(source, sourceOffset, sourceLength, true);
        Map<String, Object> parsedSource = dehydrateSource(mapTuple.v2());
        if(parsedSource != null) {
            CachedStreamOutput.Entry cachedEntry = CachedStreamOutput.popEntry();
            try {
                StreamOutput streamOutput = cachedEntry.cachedBytes();
                XContentBuilder builder = XContentFactory.contentBuilder(mapTuple.v1(), streamOutput).map(parsedSource);
                builder.close();
                return new BytesHolder(cachedEntry.bytes().copiedByteArray());
            } finally {
                CachedStreamOutput.pushEntry(cachedEntry);
            }
        } else {
            return null;
        }
    }

    @Override
    public BytesHolder rehydrateSource(String type, String id, byte[] source, int sourceOffset, int sourceLength) {
        Tuple<XContentType, Map<String, Object>> mapTuple = XContentHelper.convertToMap(source, sourceOffset, sourceLength, true);
        Map<String, Object> sourceMap = mapTuple.v2();
        Object pathFieldObject = sourceMap.get(pathField);
        if (pathFieldObject != null && pathFieldObject instanceof String) {
            // Load source from the path
            return loadFile(id, type, (String) pathFieldObject);
        } else {
            // Path field is not found - don't load source
            return null;
        }
    }

    @Override
    public boolean enabled() {
        return true;
    }

    private BytesHolder loadFile(String id, String type, String path) {
        // Emulate loading source from the path
        byte[] buffer = String.format(sourcePattern, id, type, path).getBytes();
        return new BytesHolder(buffer);
    }

}
