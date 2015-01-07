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
package org.elasticsearch.cluster.metadata;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.elasticsearch.cluster.AbstractClusterStatePart;
import org.elasticsearch.cluster.LocalContext;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.compress.CompressedString;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.common.xcontent.XContentParser.Token;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Map;

/**
 */
public class CompressedMappingMetaData extends AbstractClusterStatePart implements IndexClusterStatePart<CompressedMappingMetaData> {

    public static final String TYPE = "mappings";

    public static final Factory FACTORY = new Factory();

    @Override
    public CompressedMappingMetaData mergeWith(CompressedMappingMetaData second) {
        throw new UnsupportedOperationException("merge should occur in MetaDataCreateIndexService");
    }

    @Override
    public String partType() {
        return TYPE;
    }

    public static class Factory extends AbstractClusterStatePart.AbstractFactory<CompressedMappingMetaData> {

        @Override
        public CompressedMappingMetaData readFrom(StreamInput in, LocalContext context) throws IOException {
            ImmutableOpenMap.Builder<String, CompressedString> builder = ImmutableOpenMap.builder();
            int mappingsSize = in.readVInt();
            for (int i = 0; i < mappingsSize; i++) {
                builder.put(in.readString(), CompressedString.readCompressedString(in));
            }
            return new CompressedMappingMetaData(builder.build());
        }

        @Override
        public void writeTo(CompressedMappingMetaData mappings, StreamOutput out) throws IOException {
            out.writeVInt(mappings.mappings().size());
            for (ObjectObjectCursor<String, CompressedString> cursor : mappings.mappings()) {
                out.writeString(cursor.key);
                cursor.value.writeTo(out);
            }
        }

        @Override
        public String partType() {
            return TYPE;
        }

        @Override
        public void toXContent(CompressedMappingMetaData mappings, XContentBuilder builder, ToXContent.Params params) throws IOException {
            if (params.paramAsBoolean("reduce_mappings", false)) {
                builder.startObject();
                for (ObjectObjectCursor<String, CompressedString> cursor : mappings.mappings()) {
                    byte[] mappingSource = cursor.value.uncompressed();
                    XContentParser parser = XContentFactory.xContent(mappingSource).createParser(mappingSource);
                    Map<String, Object> mapping = parser.map();
                    if (mapping.size() == 1 && mapping.containsKey(cursor.key)) {
                        // the type name is the root value, reduce it
                        mapping = (Map<String, Object>) mapping.get(cursor.key);
                    }
                    builder.field(cursor.key);
                    builder.map(mapping);
                }
                builder.endObject();
            } else {
                builder.startArray();
                for (ObjectObjectCursor<String, CompressedString> cursor : mappings.mappings()) {
                    byte[] data = cursor.value.uncompressed();
                    XContentParser parser = XContentFactory.xContent(data).createParser(data);
                    Map<String, Object> mapping = parser.mapOrderedAndClose();
                    builder.map(mapping);
                }
                builder.endArray();
            }
        }

        @Override
        public CompressedMappingMetaData fromXContent(XContentParser parser, LocalContext context) throws IOException {
            ImmutableOpenMap.Builder<String, CompressedString> builder = ImmutableOpenMap.builder();
            XContentParser.Token token = parser.currentToken();
            String currentFieldName = null;
            if (token == Token.START_OBJECT) {
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                    } else if (token == XContentParser.Token.START_OBJECT) {
                        String mappingType = currentFieldName;
                        Map<String, Object> mappingSource = MapBuilder.<String, Object>newMapBuilder().put(mappingType, parser.mapOrdered()).map();
                        builder.put(mappingType,  new CompressedString(XContentFactory.jsonBuilder().map(mappingSource).string()));
                    }
                }
            } else if (token == Token.START_ARRAY) {
                while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                    Map<String, Object> mapping = parser.mapOrdered();
                    if (mapping.size() == 1) {
                        String mappingType = mapping.keySet().iterator().next();
                        String mappingSource = XContentFactory.jsonBuilder().map(mapping).string();

                        if (mappingSource == null) {
                            // crap, no mapping source, warn?
                        } else {
                            builder.put(mappingType, new CompressedString(mappingSource));
                        }
                    }
                }
            }
            return new CompressedMappingMetaData(builder.build());
        }

        public CompressedMappingMetaData fromOpenMap(ImmutableOpenMap<String, CompressedString> map) {
            return new CompressedMappingMetaData(map);
        }

        @Override
        public EnumSet<XContentContext> context() {
            return API_GATEWAY_SNAPSHOT;
        }
    }

    private ImmutableOpenMap<String, CompressedString> mappings;

    public CompressedMappingMetaData(ImmutableOpenMap<String, CompressedString> mappings) {
        this.mappings = mappings;
    }

    public ImmutableOpenMap<String, CompressedString> getMappings() {
        return mappings;
    }

    public ImmutableOpenMap<String, CompressedString> mappings() {
        return mappings;
    }
}
