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

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.cluster.AbstractDiffable;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentSubParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.mapper.DocumentMapper;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * Mapping configuration for a type.
 */
public class MappingMetadata extends AbstractDiffable<MappingMetadata> {

    public static final MappingMetadata EMPTY_MAPPINGS = new MappingMetadata("_doc", Collections.emptyMap());

    private final String type;

    private final CompressedXContent source;

    private final boolean routingRequired;

    public MappingMetadata(DocumentMapper docMapper) {
        this.type = docMapper.type();
        this.source = docMapper.mappingSource();
        this.routingRequired = docMapper.routingFieldMapper().required();
    }

    // xcontent is always of type JSON and wrapped in a type object
    public MappingMetadata(BytesReference mapping) {
        String type = null;
        boolean routingRequired = false;
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY,
            DeprecationHandler.IGNORE_DEPRECATIONS, mapping.streamInput())) {
            while (parser.nextToken() != null) {
                if (parser.currentToken() == XContentParser.Token.FIELD_NAME) {
                    if (type != null) {
                        throw new IllegalStateException("Mappings must contain a single type root, but found ["
                            + type + "," + parser.currentName() + "]");
                    }
                    type = parser.currentName();
                    parser.nextToken();
                    try (XContentParser sub = new XContentSubParser(parser)) {
                        routingRequired = findRoutingRequired(sub);
                    }
                }
            }
            this.source = new CompressedXContent(mapping);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        if (type == null) {
            throw new IllegalStateException("Mappings must contain a single type root");
        }
        this.type = type;
        this.routingRequired = routingRequired;
    }

    public MappingMetadata(String type, Map<String, Object> mapping) {
        this(buildXContent(type, mapping));
    }

    private static BytesReference buildXContent(String type, Map<String, Object> mapping) {
        try {
            if (mapping.isEmpty()) {
                return new BytesArray("{\"" + type + "\":{}}");
            }
            if (mapping.size() > 1 || mapping.keySet().iterator().next().equals(type) == false) {
                mapping = Map.of(type, mapping);
            }
            return BytesReference.bytes(XContentFactory.jsonBuilder().map(mapping));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static boolean findRoutingRequired(XContentParser parser) throws IOException {
        while (parser.nextToken() != null) {
            if (parser.currentToken() == XContentParser.Token.FIELD_NAME) {
                if ("_routing".equals(parser.currentName()) == false) {
                    parser.skipChildren();
                }
                if ("required".equals(parser.currentName())) {
                    parser.nextToken();
                    return parser.booleanValue();
                }
            }
        }
        return false;
    }

    public String type() {
        return this.type;
    }

    public CompressedXContent source() {
        return this.source;
    }

    /**
     * Converts the serialized compressed form of the mappings into a parsed map.
     */
    @SuppressWarnings("unchecked")
    public Map<String, Object> sourceAsMap() throws ElasticsearchParseException {
        Map<String, Object> mapping = XContentHelper.convertToMap(source.compressedReference(), true).v2();
        if (mapping.size() == 1 && mapping.containsKey(type())) {
            // the type name is the root value, reduce it
            mapping = (Map<String, Object>) mapping.get(type());
        }
        return mapping;
    }

    /**
     * Converts the serialized compressed form of the mappings into a parsed map.
     */
    public Map<String, Object> getSourceAsMap() throws ElasticsearchParseException {
        return sourceAsMap();
    }

    public boolean routingRequired() {
        return this.routingRequired;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(type());
        source().writeTo(out);
        // routing
        out.writeBoolean(routingRequired);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MappingMetadata that = (MappingMetadata) o;

        if (!Objects.equals(this.routingRequired, that.routingRequired)) return false;
        if (!source.equals(that.source)) return false;
        if (!type.equals(that.type)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, source, routingRequired);
    }

    public MappingMetadata(StreamInput in) throws IOException {
        type = in.readString();
        source = CompressedXContent.readCompressedString(in);
        routingRequired = in.readBoolean();
    }

    public static Diff<MappingMetadata> readDiffFrom(StreamInput in) throws IOException {
        return readDiffFrom(MappingMetadata::new, in);
    }
}
