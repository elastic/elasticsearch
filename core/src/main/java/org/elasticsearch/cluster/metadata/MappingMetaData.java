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

import org.elasticsearch.Version;
import org.elasticsearch.action.TimestampParsingException;
import org.elasticsearch.cluster.AbstractDiffable;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.joda.FormatDateTimeFormatter;
import org.elasticsearch.common.joda.Joda;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.internal.TimestampFieldMapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.lenientNodeBooleanValue;

/**
 * Mapping configuration for a type.
 */
public class MappingMetaData extends AbstractDiffable<MappingMetaData> {

    public static final MappingMetaData PROTO = new MappingMetaData();

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

    public static class Timestamp {

        private static final FormatDateTimeFormatter EPOCH_MILLIS_PARSER = Joda.forPattern("epoch_millis");

        public static String parseStringTimestamp(String timestampAsString, FormatDateTimeFormatter dateTimeFormatter) throws TimestampParsingException {
            try {
                return Long.toString(dateTimeFormatter.parser().parseMillis(timestampAsString));
            } catch (RuntimeException e) {
                throw new TimestampParsingException(timestampAsString, e);
            }
        }


        public static final Timestamp EMPTY = new Timestamp(false, TimestampFieldMapper.DEFAULT_DATE_TIME_FORMAT,
                TimestampFieldMapper.Defaults.DEFAULT_TIMESTAMP, null);

        private final boolean enabled;

        private final String format;

        private final FormatDateTimeFormatter dateTimeFormatter;

        private final String defaultTimestamp;

        private final Boolean ignoreMissing;

        public Timestamp(boolean enabled, String format, String defaultTimestamp, Boolean ignoreMissing) {
            this.enabled = enabled;
            this.format = format;
            this.dateTimeFormatter = Joda.forPattern(format);
            this.defaultTimestamp = defaultTimestamp;
            this.ignoreMissing = ignoreMissing;
        }

        public boolean enabled() {
            return enabled;
        }

        public String format() {
            return this.format;
        }

        public String defaultTimestamp() {
            return this.defaultTimestamp;
        }

        public boolean hasDefaultTimestamp() {
            return this.defaultTimestamp != null;
        }

        public Boolean ignoreMissing() {
            return ignoreMissing;
        }

        public FormatDateTimeFormatter dateTimeFormatter() {
            return this.dateTimeFormatter;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Timestamp timestamp = (Timestamp) o;

            if (enabled != timestamp.enabled) return false;
            if (format != null ? !format.equals(timestamp.format) : timestamp.format != null) return false;
            if (defaultTimestamp != null ? !defaultTimestamp.equals(timestamp.defaultTimestamp) : timestamp.defaultTimestamp != null) return false;
            if (ignoreMissing != null ? !ignoreMissing.equals(timestamp.ignoreMissing) : timestamp.ignoreMissing != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = (enabled ? 1 : 0);
            result = 31 * result + (format != null ? format.hashCode() : 0);
            result = 31 * result + (dateTimeFormatter != null ? dateTimeFormatter.hashCode() : 0);
            result = 31 * result + (defaultTimestamp != null ? defaultTimestamp.hashCode() : 0);
            result = 31 * result + (ignoreMissing != null ? ignoreMissing.hashCode() : 0);
            return result;
        }
    }

    private final String type;

    private final CompressedXContent source;

    private Routing routing;
    private Timestamp timestamp;
    private boolean hasParentField;

    public MappingMetaData(DocumentMapper docMapper) {
        this.type = docMapper.type();
        this.source = docMapper.mappingSource();
        this.routing = new Routing(docMapper.routingFieldMapper().required());
        this.timestamp = new Timestamp(docMapper.timestampFieldMapper().enabled(),
                docMapper.timestampFieldMapper().fieldType().dateTimeFormatter().format(), docMapper.timestampFieldMapper().defaultTimestamp(),
                docMapper.timestampFieldMapper().ignoreMissing());
        this.hasParentField = docMapper.parentFieldMapper().active();
    }

    public MappingMetaData(CompressedXContent mapping) throws IOException {
        this.source = mapping;
        Map<String, Object> mappingMap;
        try (XContentParser parser = XContentHelper.createParser(mapping.compressedReference())) {
            mappingMap = parser.mapOrdered();
        }
        if (mappingMap.size() != 1) {
            throw new IllegalStateException("Can't derive type from mapping, no root type: " + mapping.string());
        }
        this.type = mappingMap.keySet().iterator().next();
        initMappers((Map<String, Object>) mappingMap.get(this.type));
    }

    public MappingMetaData(Map<String, Object> mapping) throws IOException {
        this(mapping.keySet().iterator().next(), mapping);
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
                    required = lenientNodeBooleanValue(fieldNode);
                }
            }
            this.routing = new Routing(required);
        } else {
            this.routing = Routing.EMPTY;
        }
        if (withoutType.containsKey("_timestamp")) {
            boolean enabled = false;
            String format = TimestampFieldMapper.DEFAULT_DATE_TIME_FORMAT;
            String defaultTimestamp = TimestampFieldMapper.Defaults.DEFAULT_TIMESTAMP;
            Boolean ignoreMissing = null;
            Map<String, Object> timestampNode = (Map<String, Object>) withoutType.get("_timestamp");
            for (Map.Entry<String, Object> entry : timestampNode.entrySet()) {
                String fieldName = entry.getKey();
                Object fieldNode = entry.getValue();
                if (fieldName.equals("enabled")) {
                    enabled = lenientNodeBooleanValue(fieldNode);
                } else if (fieldName.equals("format")) {
                    format = fieldNode.toString();
                } else if (fieldName.equals("default") && fieldNode != null) {
                    defaultTimestamp = fieldNode.toString();
                } else if (fieldName.equals("ignore_missing")) {
                    ignoreMissing = lenientNodeBooleanValue(fieldNode);
                }
            }
            this.timestamp = new Timestamp(enabled, format, defaultTimestamp, ignoreMissing);
        } else {
            this.timestamp = Timestamp.EMPTY;
        }
        if (withoutType.containsKey("_parent")) {
            this.hasParentField = true;
        } else {
            this.hasParentField = false;
        }
    }

    public MappingMetaData(String type, CompressedXContent source, Routing routing, Timestamp timestamp, boolean hasParentField) {
        this.type = type;
        this.source = source;
        this.routing = routing;
        this.timestamp = timestamp;
        this.hasParentField = hasParentField;
    }

    void updateDefaultMapping(MappingMetaData defaultMapping) {
        if (routing == Routing.EMPTY) {
            routing = defaultMapping.routing();
        }
        if (timestamp == Timestamp.EMPTY) {
            timestamp = defaultMapping.timestamp();
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
    public Map<String, Object> sourceAsMap() throws IOException {
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
    public Map<String, Object> getSourceAsMap() throws IOException {
        return sourceAsMap();
    }

    public Routing routing() {
        return this.routing;
    }

    public Timestamp timestamp() {
        return this.timestamp;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(type());
        source().writeTo(out);
        // routing
        out.writeBoolean(routing().required());
        // timestamp
        out.writeBoolean(timestamp().enabled());
        out.writeString(timestamp().format());
        out.writeOptionalString(timestamp().defaultTimestamp());
        out.writeOptionalBoolean(timestamp().ignoreMissing());
        out.writeBoolean(hasParentField());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MappingMetaData that = (MappingMetaData) o;

        if (!routing.equals(that.routing)) return false;
        if (!source.equals(that.source)) return false;
        if (!timestamp.equals(that.timestamp)) return false;
        if (!type.equals(that.type)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = type.hashCode();
        result = 31 * result + source.hashCode();
        result = 31 * result + routing.hashCode();
        result = 31 * result + timestamp.hashCode();
        return result;
    }

    public MappingMetaData readFrom(StreamInput in) throws IOException {
        String type = in.readString();
        CompressedXContent source = CompressedXContent.readCompressedString(in);
        // routing
        Routing routing = new Routing(in.readBoolean());
        // timestamp

        boolean enabled = in.readBoolean();
        String format = in.readString();
        String defaultTimestamp = in.readOptionalString();
        Boolean ignoreMissing = null;

        ignoreMissing = in.readOptionalBoolean();

        final Timestamp timestamp = new Timestamp(enabled, format, defaultTimestamp, ignoreMissing);
        final boolean hasParentField = in.readBoolean();
        return new MappingMetaData(type, source, routing, timestamp, hasParentField);
    }

}
