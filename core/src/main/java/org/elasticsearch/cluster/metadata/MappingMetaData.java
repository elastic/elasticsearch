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
import org.elasticsearch.Version;
import org.elasticsearch.cluster.AbstractDiffable;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.DocumentMapper;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeBooleanValue;

/**
 * Mapping configuration for a type.
 */
public class MappingMetaData extends AbstractDiffable<MappingMetaData> {

    public static class Routing {

        public static final Routing EMPTY = new Routing(false);

        private final boolean required;

        public Routing(boolean required) {
            this.required = required;
        }

        public boolean required() {
            return required;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Routing routing = (Routing) o;

            return required == routing.required;
        }

        @Override
        public int hashCode() {
            return getClass().hashCode() + (required ? 1 : 0);
        }
    }

    private final String type;

    private final CompressedXContent source;

    private Routing routing;
    private boolean hasParentField;

    public MappingMetaData(DocumentMapper docMapper) {
        this.type = docMapper.type();
        this.source = docMapper.mappingSource();
        this.routing = new Routing(docMapper.routingFieldMapper().required());
        this.hasParentField = docMapper.parentFieldMapper().active();
    }

    public MappingMetaData(CompressedXContent mapping) throws IOException {
        this.source = mapping;
        Map<String, Object> mappingMap = XContentHelper.convertToMap(mapping.compressedReference(), true).v2();
        if (mappingMap.size() != 1) {
            throw new IllegalStateException("Can't derive type from mapping, no root type: " + mapping.string());
        }
        this.type = mappingMap.keySet().iterator().next();
        initMappers((Map<String, Object>) mappingMap.get(this.type));
    }

    public MappingMetaData(String type, Map<String, Object> mapping) throws IOException {
        this.type = type;
        XContentBuilder mappingBuilder = XContentFactory.jsonBuilder().map(mapping);
        this.source = new CompressedXContent(mappingBuilder.bytes());
        Map<String, Object> withoutType = mapping;
        if (mapping.size() == 1 && mapping.containsKey(type)) {
            withoutType = (Map<String, Object>) mapping.get(type);
        }
        initMappers(withoutType);
    }

    private MappingMetaData() {
        this.type = "";
        try {
            this.source = new CompressedXContent("{}");
        } catch (IOException ex) {
            throw new IllegalStateException("Cannot create MappingMetaData prototype", ex);
        }
    }

    private void initMappers(Map<String, Object> withoutType) {
        if (withoutType.containsKey("_routing")) {
            boolean required = false;
            Map<String, Object> routingNode = (Map<String, Object>) withoutType.get("_routing");
            for (Map.Entry<String, Object> entry : routingNode.entrySet()) {
                String fieldName = entry.getKey();
                Object fieldNode = entry.getValue();
                if (fieldName.equals("required")) {
                    try {
                        required = nodeBooleanValue(fieldNode);
                    } catch (IllegalArgumentException ex) {
                        throw new IllegalArgumentException("Failed to create mapping for type [" + this.type() + "]. " +
                            "Illegal value in field [_routing.required].", ex);
                    }
                }
            }
            this.routing = new Routing(required);
        } else {
            this.routing = Routing.EMPTY;
        }
        if (withoutType.containsKey("_parent")) {
            this.hasParentField = true;
        } else {
            this.hasParentField = false;
        }
    }

    public MappingMetaData(String type, CompressedXContent source, Routing routing, boolean hasParentField) {
        this.type = type;
        this.source = source;
        this.routing = routing;
        this.hasParentField = hasParentField;
    }

    void updateDefaultMapping(MappingMetaData defaultMapping) {
        if (routing == Routing.EMPTY) {
            routing = defaultMapping.routing();
        }
    }

    public String type() {
        return this.type;
    }

    public CompressedXContent source() {
        return this.source;
    }

    public boolean hasParentField() {
        return hasParentField;
    }

    /**
     * Converts the serialized compressed form of the mappings into a parsed map.
     */
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

    public Routing routing() {
        return this.routing;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(type());
        source().writeTo(out);
        // routing
        out.writeBoolean(routing().required());
        if (out.getVersion().before(Version.V_6_0_0_alpha1)) {
            // timestamp
            out.writeBoolean(false); // enabled
            out.writeString(DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.format());
            out.writeOptionalString("now"); // 5.x default
            out.writeOptionalBoolean(null);
        }
        out.writeBoolean(hasParentField());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MappingMetaData that = (MappingMetaData) o;

        if (!routing.equals(that.routing)) return false;
        if (!source.equals(that.source)) return false;
        if (!type.equals(that.type)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = type.hashCode();
        result = 31 * result + source.hashCode();
        result = 31 * result + routing.hashCode();
        return result;
    }

    public MappingMetaData(StreamInput in) throws IOException {
        type = in.readString();
        source = CompressedXContent.readCompressedString(in);
        // routing
        routing = new Routing(in.readBoolean());
        if (in.getVersion().before(Version.V_6_0_0_alpha1)) {
            // timestamp
            boolean enabled = in.readBoolean();
            if (enabled) {
                throw new IllegalArgumentException("_timestamp may not be enabled");
            }
            in.readString(); // format
            in.readOptionalString(); // defaultTimestamp
            in.readOptionalBoolean(); // ignoreMissing
        }
        hasParentField = in.readBoolean();
    }

    public static Diff<MappingMetaData> readDiffFrom(StreamInput in) throws IOException {
        return readDiffFrom(MappingMetaData::new, in);
    }

}
