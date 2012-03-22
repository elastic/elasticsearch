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
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.mapper.MergeContext;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.source.SourceProvider;
import org.elasticsearch.index.source.SourceProviderParser;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.*;

/**
 * Emulates a source provider that can rehydrate source based on type and id
 */
public class IdBasedSourceProviderParser implements SourceProviderParser {

    public static class Defaults {
        public static String NAME = "idbased";
    }

    @Override public SourceProvider parse(Map<String, Object> node) {
        String sourcePattern = "{\"_id\":\"%1$s\", \"_type\":\"%2$s\", \"body\":\"This record has id %1$s and type %2$s\"}";

        for (Map.Entry<String, Object> entry : node.entrySet()) {
            String fieldName = Strings.toUnderscoreCase(entry.getKey());
            Object fieldNode = entry.getValue();
            if (fieldName.equals("source_pattern") && fieldNode != null) {
                sourcePattern = nodeStringValue(fieldNode, null);
            }
        }
        return new IdBasedSourceProvider(sourcePattern);
    }

    private static class IdBasedSourceProvider implements SourceProvider {

        private final String sourcePattern;

        public IdBasedSourceProvider(String sourcePattern) {
            this.sourcePattern = sourcePattern;

        }

        @Override public BytesHolder dehydrateSource(ParseContext context) throws IOException {
            // Placeholder to indicate that source is present
            return BytesHolder.EMPTY;

        }

        @Override public String name() {
            return Defaults.NAME;
        }

        @Override public BytesHolder rehydrateSource(String type, String id, byte[] source, int sourceOffset, int sourceLength) {
            // Generate source based on type and id
            byte[] buffer = String.format(sourcePattern, type, id).getBytes();
            return new BytesHolder(buffer);
        }

        @Override public void merge(SourceProvider mergeWith, MergeContext mergeContext) {
            // Ignore changes
        }

        @Override public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            IdBasedSourceProvider that = (IdBasedSourceProvider) o;

            if (sourcePattern != null ? !sourcePattern.equals(that.sourcePattern) : that.sourcePattern != null)
                return false;

            return true;
        }

        @Override
        public int hashCode() {
            return sourcePattern != null ? sourcePattern.hashCode() : 0;
        }
    }
}
