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
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.hash.MessageDigests;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeBooleanValue;

/**
 * Mapping configuration for a type.
 */
public class MappingMetadata extends AbstractDiffable<MappingMetadata> {

    public static final MappingMetadata EMPTY_MAPPINGS = new MappingMetadata("_doc", Collections.emptyMap());

    private final String type;

    private final CompressedXContent source;

    private final boolean routingRequired;

    private final String digest;

    public MappingMetadata(DocumentMapper docMapper) {
        this.type = docMapper.type();
        this.source = docMapper.mappingSource();
        this.routingRequired = docMapper.routingFieldMapper().required();
        this.digest = computeDigest(source);
    }

    public MappingMetadata(CompressedXContent mapping) {
        this.source = mapping;
        Map<String, Object> mappingMap = XContentHelper.convertToMap(mapping.compressedReference(), true).v2();
        if (mappingMap.size() != 1) {
            throw new IllegalStateException("Can't derive type from mapping, no root type: " + mapping.string());
        }
        this.type = mappingMap.keySet().iterator().next();
        this.routingRequired = routingRequired((Map<?, ?>) mappingMap.get(this.type));
        this.digest = computeDigest(mapping);
    }

    public MappingMetadata(String type, Map<String, Object> mapping) {
        this.type = type;
        String mappingAsString = Strings.toString((builder, params) -> builder.mapContents(mapping));
        try {
            this.source = new CompressedXContent(mappingAsString);
        } catch (IOException e) {
            throw new UncheckedIOException(e);  // XContent exception, should never happen
        }
        Map<?, ?> withoutType = mapping;
        if (mapping.size() == 1 && mapping.containsKey(type)) {
            withoutType = (Map<?, ?>) mapping.get(type);
        }
        this.routingRequired = routingRequired(withoutType);
        this.digest = computeDigest(mappingAsString);
    }

    static String computeDigest(CompressedXContent source) {
        try {
            String mapping = XContentHelper.convertToJson(source.uncompressed(), false, XContentType.JSON);
            return computeDigest(mapping);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    static String computeDigest(String mapping) {
        return MessageDigests.toHexString(MessageDigests.sha256().digest(mapping.getBytes(StandardCharsets.UTF_8)));
    }

    public static void writeMappingMetadata(StreamOutput out, ImmutableOpenMap<String, MappingMetadata> mappings) throws IOException {
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

    private boolean routingRequired(Map<?, ?> withoutType) {
        boolean required = false;
        if (withoutType.containsKey("_routing")) {
            Map<?, ?> routingNode = (Map<?, ?>) withoutType.get("_routing");
            for (Map.Entry<?, ?> entry : routingNode.entrySet()) {
                String fieldName = (String) entry.getKey();
                Object fieldNode = entry.getValue();
                if (fieldName.equals("required")) {
                    try {
                        required = nodeBooleanValue(fieldNode);
                    } catch (IllegalArgumentException ex) {
                        throw new IllegalArgumentException(
                            "Failed to create mapping for type [" + this.type() + "]. " + "Illegal value in field [_routing.required].",
                            ex
                        );
                    }
                }
            }
        }
        return required;
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

    public String getDigest() {
        return digest;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(type());
        source().writeTo(out);
        // routing
        out.writeBoolean(routingRequired);
        if (out.getVersion().onOrAfter(Version.V_8_1_0)) {
            out.writeString(digest);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MappingMetadata that = (MappingMetadata) o;

        if (Objects.equals(this.routingRequired, that.routingRequired) == false) return false;
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
        digest = in.getVersion().onOrAfter(Version.V_8_1_0) ? in.readString() : computeDigest(source);
    }

    public static Diff<MappingMetadata> readDiffFrom(StreamInput in) throws IOException {
        return readDiffFrom(MappingMetadata::new, in);
    }
}
