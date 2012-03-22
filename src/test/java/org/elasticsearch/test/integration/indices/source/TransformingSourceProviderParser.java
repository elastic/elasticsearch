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

import com.google.common.collect.Maps;
import org.elasticsearch.common.BytesHolder;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.mapper.MergeContext;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.source.SourceProvider;
import org.elasticsearch.index.source.SourceProviderParser;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.*;

/**
 * Emulates source provider that needs a content of a field in order to restore source
 */
public class TransformingSourceProviderParser implements SourceProviderParser {

    public static class Defaults {
        public static String NAME = "transform";
        public static String SOURCE_PATTERN = "{\"_id\":\"%1$s\", \"_type\":\"%2$s\", \"body\":\"Content of a file %3$s\", " +
                "\"file\":\"%3$s\", \"generated\":true}";
        public static String PATH_FIELD = "file";
    }

    @Override public SourceProvider parse(Map<String, Object> node) {
        String sourcePattern = Defaults.SOURCE_PATTERN;
        String pathField = Defaults.PATH_FIELD;

        for (Map.Entry<String, Object> entry : node.entrySet()) {
            String fieldName = Strings.toUnderscoreCase(entry.getKey());
            Object fieldNode = entry.getValue();
            if (fieldName.equals("source_pattern") && fieldNode != null) {
                sourcePattern = nodeStringValue(fieldNode, null);
            } else if (fieldName.equals("path_field") && fieldNode != null) {
                pathField = nodeStringValue(fieldNode, null);
            }

        }
        return new TransformingSourceProvider(sourcePattern, pathField);
    }

    private static class TransformingSourceProvider implements SourceProvider {

        private final String sourcePattern;
        private final String pathField;

        public TransformingSourceProvider(String sourcePattern, String pathField) {
            this.sourcePattern = sourcePattern;
            this.pathField = pathField;

        }

        @Override public BytesHolder dehydrateSource(ParseContext context) throws IOException {
            // Extract content of the path field
            Tuple<XContentType, Map<String, Object>> mapTuple = XContentHelper.convertToMap(context.source(),
                    context.sourceOffset(), context.sourceLength(), true);
            Map<String, Object> sourceMap = mapTuple.v2();
            Object pathFieldObject = sourceMap.get(pathField);
            if (pathFieldObject != null && pathFieldObject instanceof String) {
                // Replace path with just the portion that is needed to restore source in the future
                Map<String, Object> dehydratedMap = Maps.newHashMap();
                dehydratedMap.put(pathField, pathFieldObject);
                XContentBuilder builder = XContentFactory.contentBuilder(mapTuple.v1()).map(dehydratedMap);
                return new BytesHolder(builder.copiedBytes());
            } else {
                // Path field is not found - don't store source at all
                return null;
            }
        }

        @Override public String name() {
            return Defaults.NAME;
        }

        @Override public BytesHolder rehydrateSource(String type, String id, byte[] source, int sourceOffset, int sourceLength) {
            // Extract file path from source
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

        @Override public void merge(SourceProvider mergeWith, MergeContext mergeContext) {
            // Ignore changes
        }

        @Override public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            if (!Defaults.PATH_FIELD.equals(pathField)) {
                builder.field("path_field", pathField);
            }
            if (!Defaults.SOURCE_PATTERN.equals(sourcePattern)) {
                builder.field("source_pattern", sourcePattern);
            }
            return builder;
        }

        private BytesHolder loadFile(String id, String type, String path) {
            // Emulate loading source from the path
            byte[] buffer = String.format(sourcePattern, id, type, path).getBytes();
            return new BytesHolder(buffer);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            TransformingSourceProvider that = (TransformingSourceProvider) o;

            if (pathField != null ? !pathField.equals(that.pathField) : that.pathField != null) return false;
            if (sourcePattern != null ? !sourcePattern.equals(that.sourcePattern) : that.sourcePattern != null)
                return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = sourcePattern != null ? sourcePattern.hashCode() : 0;
            result = 31 * result + (pathField != null ? pathField.hashCode() : 0);
            return result;
        }
    }
}

