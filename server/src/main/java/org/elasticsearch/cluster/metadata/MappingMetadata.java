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
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.SimpleDiffable;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeBooleanValue;

/**
 * Mapping configuration for a type.
 */
public class MappingMetadata implements SimpleDiffable<MappingMetadata>, ToXContentFragment {

    public static final MappingMetadata EMPTY_MAPPINGS = new MappingMetadata(
        MapperService.SINGLE_MAPPING_NAME,
        Map.of(MapperService.SINGLE_MAPPING_NAME, Map.of())
    );

    // TODO split this into CompressedXContent and Map-backed impls, similar to Source
    // we end up serializing and deserializing stuff for no reason in far too many places

    private final String type;

    // always includes the type at top-level
    private final CompressedXContent source;

    private Boolean routingRequired = null;

    public MappingMetadata(DocumentMapper docMapper) {
        this.type = docMapper.type();
        this.source = docMapper.mappingSource();
        this.routingRequired = docMapper.routingFieldMapper().required();
    }

    /**
     * Create MappingMetadata from compressed XContent
     *
     * The xcontent should have a containing map whose key is a type - typically '_doc'. This
     * will be discarded when the mapping is returned via {@link #sourceAsMap()}
     */
    public MappingMetadata(CompressedXContent mapping) {
        this.source = mapping;
        // TODO do this via pull parsing - we don't need to reify the whole map!
        Map<String, Object> mappingMap = XContentHelper.convertToMap(mapping.compressedReference(), true).v2();
        if (mappingMap.isEmpty()) {
            this.type = MapperService.SINGLE_MAPPING_NAME;
        } else if (mappingMap.size() != 1) {
            throw new IllegalStateException("Can't derive type from mapping, no root type: " + mapping.string());
        } else {
            this.type = mappingMap.keySet().iterator().next();
        }
    }

    /**
     * Create MappingMetadata from a java map
     * @param type      the type, typically _doc
     * @param mapping   the mappings, which may or not be wrapped with the type
     */
    public MappingMetadata(String type, Map<String, Object> mapping) {
        this.type = type;
        try {
            if (mapping.size() != 1 || mapping.containsKey(this.type) == false) {
                mapping = Map.of(this.type, mapping);
            }
            this.source = new CompressedXContent(mapping);
        } catch (IOException e) {
            throw new UncheckedIOException(e);  // XContent exception, should never happen
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(type);
        builder.map(sourceAsMap());
        return builder;
    }

    public static void writeMappingMetadata(StreamOutput out, Map<String, MappingMetadata> mappings) throws IOException {
        out.writeMap(mappings, StreamOutput::writeString, out.getVersion().before(Version.V_8_0_0) ? (o, v) -> {
            o.writeVInt(v == EMPTY_MAPPINGS ? 0 : 1);
            if (v != EMPTY_MAPPINGS) {
                o.writeString(MapperService.SINGLE_MAPPING_NAME);
                v.writeTo(o);
            }
        } : (o, v) -> {
            o.writeBoolean(v != EMPTY_MAPPINGS);
            if (v != EMPTY_MAPPINGS) {
                v.writeTo(o);
            }
        });
    }

    @SuppressWarnings("unchecked")
    private static boolean routingRequired(Map<String, Object> withoutType) {
        boolean required = false;
        if (withoutType.containsKey("_routing")) {
            Map<String, Object> routingNode = (Map<String, Object>) withoutType.get("_routing");
            for (Map.Entry<String, Object> entry : routingNode.entrySet()) {
                String fieldName = entry.getKey();
                Object fieldNode = entry.getValue();
                if (fieldName.equals("required")) {
                    try {
                        required = nodeBooleanValue(fieldNode);
                    } catch (IllegalArgumentException ex) {
                        throw new IllegalArgumentException("Failed to create mapping: illegal value in field [_routing.required].", ex);
                    }
                }
            }
        }
        return required;
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
        if (mapping.size() == 1 && mapping.containsKey(type)) {
            // the type name is the root value, reduce it
            mapping = (Map<String, Object>) mapping.get(type);
        }
        return mapping;
    }

    /**
     * Converts the serialized compressed form of the mappings into a parsed map.
     */
    public Map<String, Object> getSourceAsMap() throws ElasticsearchParseException {
        return sourceAsMap();
    }

    /**
     * Converts the serialized compressed form of the mappings into a parsed map.
     * In contrast to {@link #sourceAsMap()}, this does not remove the type
     */
    public Map<String, Object> rawSourceAsMap() throws ElasticsearchParseException {
        return XContentHelper.convertToMap(source.compressedReference(), true).v2();
    }

    public boolean routingRequired() {
        if (this.routingRequired == null) {
            this.routingRequired = routingRequired(sourceAsMap());
        }
        return this.routingRequired;
    }

    public String getSha256() {
        return source.getSha256();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(type);
        source().writeTo(out);
        // routing
        out.writeBoolean(routingRequired());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MappingMetadata that = (MappingMetadata) o;

        if (Objects.equals(this.routingRequired(), that.routingRequired()) == false) return false;
        if (source.equals(that.source) == false) return false;
        if (type.equals(that.type) == false) return false;

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
        return SimpleDiffable.readDiffFrom(MappingMetadata::new, in);
    }
}
