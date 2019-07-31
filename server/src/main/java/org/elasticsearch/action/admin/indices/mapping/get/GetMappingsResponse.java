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

package org.elasticsearch.action.admin.indices.mapping.get;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.BaseRestHandler;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.rest.BaseRestHandler.DEFAULT_INCLUDE_TYPE_NAME_POLICY;

public class GetMappingsResponse extends ActionResponse implements ToXContentFragment {

    private static final ParseField MAPPINGS = new ParseField("mappings");

    private ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> mappings = ImmutableOpenMap.of();

    public GetMappingsResponse(ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> mappings) {
        this.mappings = mappings;
    }

    GetMappingsResponse(StreamInput in) throws IOException {
        super(in);
        int size = in.readVInt();
        ImmutableOpenMap.Builder<String, ImmutableOpenMap<String, MappingMetaData>> indexMapBuilder = ImmutableOpenMap.builder();
        for (int i = 0; i < size; i++) {
            String key = in.readString();
            int valueSize = in.readVInt();
            ImmutableOpenMap.Builder<String, MappingMetaData> typeMapBuilder = ImmutableOpenMap.builder();
            for (int j = 0; j < valueSize; j++) {
                typeMapBuilder.put(in.readString(), new MappingMetaData(in));
            }
            indexMapBuilder.put(key, typeMapBuilder.build());
        }
        mappings = indexMapBuilder.build();
    }

    public ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> mappings() {
        return mappings;
    }

    public ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> getMappings() {
        return mappings();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(mappings.size());
        for (ObjectObjectCursor<String, ImmutableOpenMap<String, MappingMetaData>> indexEntry : mappings) {
            out.writeString(indexEntry.key);
            out.writeVInt(indexEntry.value.size());
            for (ObjectObjectCursor<String, MappingMetaData> typeEntry : indexEntry.value) {
                out.writeString(typeEntry.key);
                typeEntry.value.writeTo(out);
            }
        }
    }

    public static GetMappingsResponse fromXContent(XContentParser parser) throws IOException {
        if (parser.currentToken() == null) {
            parser.nextToken();
        }
        assert parser.currentToken() == XContentParser.Token.START_OBJECT;
        Map<String, Object> parts = parser.map();

        ImmutableOpenMap.Builder<String, ImmutableOpenMap<String, MappingMetaData>> builder = new ImmutableOpenMap.Builder<>();
        for (Map.Entry<String, Object> entry : parts.entrySet()) {
            final String indexName = entry.getKey();
            assert entry.getValue() instanceof Map : "expected a map as type mapping, but got: " + entry.getValue().getClass();
            final Map<String, Object> mapping = (Map<String, Object>) ((Map) entry.getValue()).get(MAPPINGS.getPreferredName());

            ImmutableOpenMap.Builder<String, MappingMetaData> typeBuilder = new ImmutableOpenMap.Builder<>();
            for (Map.Entry<String, Object> typeEntry : mapping.entrySet()) {
                final String typeName = typeEntry.getKey();
                assert typeEntry.getValue() instanceof Map : "expected a map as inner type mapping, but got: " +
                    typeEntry.getValue().getClass();
                final Map<String, Object> fieldMappings = (Map<String, Object>) typeEntry.getValue();
                MappingMetaData mmd = new MappingMetaData(typeName, fieldMappings);
                typeBuilder.put(typeName, mmd);
            }
            builder.put(indexName, typeBuilder.build());
        }

        return new GetMappingsResponse(builder.build());
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        boolean includeTypeName = params.paramAsBoolean(BaseRestHandler.INCLUDE_TYPE_NAME_PARAMETER,
            DEFAULT_INCLUDE_TYPE_NAME_POLICY);

        for (final ObjectObjectCursor<String, ImmutableOpenMap<String, MappingMetaData>> indexEntry : getMappings()) {
            builder.startObject(indexEntry.key);
            {
                if (includeTypeName == false) {
                    MappingMetaData mappings = null;
                    for (final ObjectObjectCursor<String, MappingMetaData> typeEntry : indexEntry.value) {
                        if (typeEntry.key.equals("_default_") == false) {
                            assert mappings == null;
                            mappings = typeEntry.value;
                        }
                    }
                    if (mappings == null) {
                        // no mappings yet
                        builder.startObject(MAPPINGS.getPreferredName()).endObject();
                    } else {
                        builder.field(MAPPINGS.getPreferredName(), mappings.sourceAsMap());
                    }
                } else {
                    builder.startObject(MAPPINGS.getPreferredName());
                    {
                        for (final ObjectObjectCursor<String, MappingMetaData> typeEntry : indexEntry.value) {
                            builder.field(typeEntry.key, typeEntry.value.sourceAsMap());
                        }
                    }
                    builder.endObject();
                }
            }
            builder.endObject();
        }
        return builder;
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    @Override
    public int hashCode() {
        return mappings.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }

        if (getClass() != obj.getClass()) {
            return false;
        }

        GetMappingsResponse other = (GetMappingsResponse) obj;
        return this.mappings.equals(other.mappings);
    }
}
