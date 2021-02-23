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
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperService;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.security.SecureRandom;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeBooleanValue;

/**
 * Mapping configuration for a type.
 */
public class MappingMetadata extends AbstractDiffable<MappingMetadata> implements ToXContent {

    public static final MappingMetadata EMPTY_MAPPINGS = new MappingMetadata("_doc", Collections.emptyMap());

    private final String type;

    private final CompressedXContent source;

    private final boolean routingRequired;

    private final Id id;

    public MappingMetadata(Id id) {
        this.id = id;
        type = null;
        routingRequired = false;
        source = null;
    }

    public MappingMetadata(DocumentMapper docMapper) {
        this.type = docMapper.type();
        this.source = docMapper.mappingSource();
        this.routingRequired = docMapper.routingFieldMapper().required();
        this.id = Id.random();
    }

    public MappingMetadata(CompressedXContent mapping) {
        this(mapping, Id.random());
    }

    @SuppressWarnings("unchecked")
    public MappingMetadata(CompressedXContent mapping, Id id) {
        this.source = mapping;
        Map<String, Object> mappingMap = XContentHelper.convertToMap(mapping.compressedReference(), true).v2();
        if (mappingMap.size() != 1) {
            throw new IllegalStateException("Can't derive type from mapping, no root type: " + mapping.string());
        }
        this.type = mappingMap.keySet().iterator().next();
        this.routingRequired = routingRequired((Map<String, Object>) mappingMap.get(this.type));
        this.id = id;
    }

    @SuppressWarnings("unchecked")
    public MappingMetadata(String type, Map<String, Object> mapping) {
        this.type = type;
        try {
            this.source = new CompressedXContent(
                    (builder, params) -> builder.mapContents(mapping), XContentType.JSON, ToXContent.EMPTY_PARAMS);
        } catch (IOException e) {
            throw new UncheckedIOException(e);  // XContent exception, should never happen
        }
        Map<String, Object> withoutType = mapping;
        if (mapping.size() == 1 && mapping.containsKey(type)) {
            withoutType = (Map<String, Object>) mapping.get(type);
        }
        this.routingRequired = routingRequired(withoutType);
        this.id = Id.random();
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
                }
        );
    }

    @SuppressWarnings("unchecked")
    private boolean routingRequired(Map<String, Object> withoutType) {
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
                        throw new IllegalArgumentException("Failed to create mapping for type [" + this.type() + "]. " +
                            "Illegal value in field [_routing.required].", ex);
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

    public Id id() {
        return id;
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
        if (out.getVersion().onOrAfter(Version.V_8_0_0)) {
            out.writeLong(id.msb);
            out.writeLong(id.lsb);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MappingMetadata that = (MappingMetadata) o;

        if (Objects.equals(this.id, that.id)) {
            return true;
        }

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
        if (in.getVersion().onOrAfter(Version.V_8_0_0)) {
            id = new Id(in.readLong(), in.readLong());
        } else {
            id = Id.random();
        }
    }

    public static Diff<MappingMetadata> readDiffFrom(StreamInput in) throws IOException {
        return readDiffFrom(MappingMetadata::new, in);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("source", source.compressed());
        builder.startArray("id");
        builder.value(id.msb);
        builder.value(id.lsb);
        builder.endArray();
        return builder;
    }

    private static final ConstructingObjectParser<MappingMetadata, Void> PARSER = new ConstructingObjectParser<>(
        "mapping",
        true,
        args -> {
            try {
                CompressedXContent mapping = new CompressedXContent((byte[]) args[0]);
                @SuppressWarnings("unchecked")
                List<Long> idList = (List<Long>) args[1];
                return new MappingMetadata(mapping, new Id(idList.get(0), idList.get(1)));
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        });
    static {
        PARSER.declareField(ConstructingObjectParser.constructorArg(), (p, c) -> p.binaryValue(), new ParseField("source"), ObjectParser.ValueType.VALUE);
        PARSER.declareLongArray(ConstructingObjectParser.constructorArg(), new ParseField("id"));
    }


    public static MappingMetadata fromXContent(XContentParser parser){
        return PARSER.apply(parser, null);
    }

    public static class Id {
        private static final SecureRandom RANDOM = new SecureRandom();
        private final long msb, lsb;

        public Id(long msb, long lsb) {
            this.msb = msb;
            this.lsb = lsb;
        }

        public long msb() {
            return msb;
        }

        public long lsb() {
            return lsb;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Id id = (Id) o;
            return msb == id.msb && lsb == id.lsb;
        }

        @Override
        public int hashCode() {
            return Objects.hash(msb, lsb);
        }

        @Override
        public String toString(){
            return String.format(Locale.ROOT, "%16x%16x", msb, lsb);
        }

        public static Id fromString(String s){
            return new Id(Long.parseLong(s.substring(0, 16), 16), Long.parseLong(s.substring(16), 16));
        }

        public static Id random(){
            return new Id(RANDOM.nextLong(), RANDOM.nextLong());
        }
    }
}
