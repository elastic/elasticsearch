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
import org.elasticsearch.cluster.ClusterStatePart;
import org.elasticsearch.cluster.ClusterStatePart.Factory;
import org.elasticsearch.cluster.LocalContext;
import org.elasticsearch.cluster.MapClusterStatePart;
import org.elasticsearch.cluster.NamedClusterStatePart;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.compress.CompressedString;
import org.elasticsearch.common.xcontent.ToXContent.Params;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilder.FieldCaseConversion;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Map;

public class MappingsMetaData  extends MapClusterStatePart<MappingMetaData> {

    public static String TYPE = "mappings";

    public static Factory FACTORY = new Factory();

    public MappingsMetaData(ImmutableOpenMap<String, MappingMetaData> parts) {
        super(TYPE, parts);
    }

    public static class Factory extends MapClusterStatePart.Factory<MappingMetaData> {

        public Factory() {
            super(MappingMetaData.TYPE, MappingMetaData.FACTORY);
        }

        @Override
        public void toXContent(MapClusterStatePart<MappingMetaData> part, XContentBuilder builder, Params params) throws IOException {
            if (params.paramAsBoolean("reduce_mappings", false)) {
                builder.startObject();
                for (ObjectObjectCursor<String, MappingMetaData> cursor : part.parts()) {
                    MappingMetaData.FACTORY.toXContent(cursor.value, builder, params);
                }
                builder.endObject();
            } else {
                builder.startArray();
                for (ObjectObjectCursor<String, MappingMetaData> cursor : part.parts()) {
                    MappingMetaData.FACTORY.toXContent(cursor.value, builder, params);
                }
                builder.endArray();
            }
        }

        @Override
        public MapClusterStatePart<MappingMetaData> fromXContent(XContentParser parser, LocalContext context) throws IOException {
            XContentParser.Token token = parser.currentToken();
            ImmutableOpenMap.Builder<String, MappingMetaData> builder = ImmutableOpenMap.builder();
            String currentFieldName = null;
            if (token == XContentParser.Token.START_OBJECT) {
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                    } else if (token == XContentParser.Token.START_OBJECT) {
                        String mappingType = currentFieldName;
                        Map<String, Object> mappingSource = MapBuilder.<String, Object>newMapBuilder().put(mappingType, parser.mapOrdered()).map();
                        builder.put(mappingType, new MappingMetaData(mappingType, mappingSource));
                    }
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                    if (token == XContentParser.Token.VALUE_EMBEDDED_OBJECT) {
                        MappingMetaData mappingMetaData = new MappingMetaData(new CompressedString(parser.binaryValue()));
                        builder.put(mappingMetaData.type(), mappingMetaData);
                    } else {
                        Map<String, Object> mapping = parser.mapOrdered();
                        if (mapping.size() == 1) {
                            String mappingType = mapping.keySet().iterator().next();
                            builder.put(mappingType, new MappingMetaData(mappingType, mapping));
                        }
                    }
                }
            }
            return new MappingsMetaData(builder.build());
        }
    }
}
