/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.AbstractDiffable;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.mapper.DocumentMapper;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeBooleanValue;

/**
 * Mapping configuration for a type.
 */
public class MappingMetadata extends AbstractDiffable<MappingMetadata> {

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

    public MappingMetadata(DocumentMapper docMapper) {
        this.type = docMapper.type();
        this.source = docMapper.mappingSource();
        this.routing = new Routing(docMapper.routingFieldMapper().required());
    }

    @SuppressWarnings("unchecked")
    public MappingMetadata(CompressedXContent mapping) {
        this.source = mapping;
        Map<String, Object> mappingMap = XContentHelper.convertToMap(mapping.compressedReference(), true).v2();
        if (mappingMap.size() != 1) {
            throw new IllegalStateException("Can't derive type from mapping, no root type: " + mapping.string());
        }
        this.type = mappingMap.keySet().iterator().next();
        initMappers((Map<String, Object>) mappingMap.get(this.type));
    }

    @SuppressWarnings("unchecked")
    public MappingMetadata(String type, Map<String, Object> mapping) throws IOException {
        this.type = type;
        this.source = new CompressedXContent(
                (builder, params) -> builder.mapContents(mapping), XContentType.JSON, ToXContent.EMPTY_PARAMS);
        Map<String, Object> withoutType = mapping;
        if (mapping.size() == 1 && mapping.containsKey(type)) {
            withoutType = (Map<String, Object>) mapping.get(type);
        }
        initMappers(withoutType);
    }

    @SuppressWarnings("unchecked")
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
    }

    void updateDefaultMapping(MappingMetadata defaultMapping) {
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
            out.writeString(DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.pattern());
            out.writeOptionalString("now"); // 5.x default
            out.writeOptionalBoolean(null);
        }
        if (out.getVersion().before(Version.V_7_0_0)) {
            out.writeBoolean(false); // hasParentField
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MappingMetadata that = (MappingMetadata) o;

        if (routing.equals(that.routing) == false) return false;
        if (source.equals(that.source) == false) return false;
        if (type.equals(that.type) == false) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = type.hashCode();
        result = 31 * result + source.hashCode();
        result = 31 * result + routing.hashCode();
        return result;
    }

    public MappingMetadata(StreamInput in) throws IOException {
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
        if (in.getVersion().before(Version.V_7_0_0)) {
            in.readBoolean(); // hasParentField
        }
    }

    public static Diff<MappingMetadata> readDiffFrom(StreamInput in) throws IOException {
        return readDiffFrom(MappingMetadata::new, in);
    }
}
