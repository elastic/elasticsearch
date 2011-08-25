/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.compress.CompressedString;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.joda.FormatDateTimeFormatter;
import org.elasticsearch.common.joda.Joda;
import org.elasticsearch.common.joda.time.format.DateTimeFormat;
import org.elasticsearch.common.joda.time.format.DateTimeFormatter;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.internal.TimestampFieldMapper;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.*;

/**
 * @author kimchy (shay.banon)
 */
public class MappingMetaData {

    public static class Routing {

        public static final Routing EMPTY = new Routing(false, null);

        private final boolean required;

        private final String path;

        private final String[] pathElements;

        public Routing(boolean required, String path) {
            this.required = required;
            this.path = path;
            if (path == null) {
                pathElements = Strings.EMPTY_ARRAY;
            } else {
                pathElements = Strings.delimitedListToStringArray(path, ".");
            }
        }

        public boolean required() {
            return required;
        }

        public boolean hasPath() {
            return path != null;
        }

        public String path() {
            return this.path;
        }

        public String[] pathElements() {
            return this.pathElements;
        }
    }

    public static class Timestamp {

        public static final Timestamp EMPTY = new Timestamp(false, null, TimestampFieldMapper.DEFAULT_DATE_TIME_FORMAT);

        private final boolean enabled;

        private final String path;

        private final String format;

        private final String[] pathElements;

        public Timestamp(boolean enabled, String path, String format) {
            this.enabled = enabled;
            this.path = path;
            if (path == null) {
                pathElements = Strings.EMPTY_ARRAY;
            } else {
                pathElements = Strings.delimitedListToStringArray(path, ".");
            }
            this.format = format;
        }

        public boolean enabled() {
            return enabled;
        }

        public boolean hasPath() {
            return path != null;
        }

        public String path() {
            return this.path;
        }

        public String[] pathElements() {
            return this.pathElements;
        }

        public String format() {
            return this.format;
        }
    }

    private final String type;

    private final CompressedString source;

    private final Routing routing;
    private final Timestamp timestamp;
    private final FormatDateTimeFormatter tsDateTimeFormatter;

    public MappingMetaData(DocumentMapper docMapper) {
        this.type = docMapper.type();
        this.source = docMapper.mappingSource();
        this.routing = new Routing(docMapper.routingFieldMapper().required(), docMapper.routingFieldMapper().path());
        this.timestamp = new Timestamp(docMapper.timestampFieldMapper().enabled(), docMapper.timestampFieldMapper().path(), docMapper.timestampFieldMapper().dateTimeFormatter().format());
        this.tsDateTimeFormatter = docMapper.timestampFieldMapper().dateTimeFormatter();
    }

    public MappingMetaData(String type, Map<String, Object> mapping) throws IOException {
        this.type = type;
        this.source = new CompressedString(XContentFactory.jsonBuilder().map(mapping).string());
        Map<String, Object> withoutType = mapping;
        if (mapping.size() == 1 && mapping.containsKey(type)) {
            withoutType = (Map<String, Object>) mapping.get(type);
        }
        if (withoutType.containsKey("_routing")) {
            boolean required = false;
            String path = null;
            Map<String, Object> routingNode = (Map<String, Object>) withoutType.get("_routing");
            for (Map.Entry<String, Object> entry : routingNode.entrySet()) {
                String fieldName = Strings.toUnderscoreCase(entry.getKey());
                Object fieldNode = entry.getValue();
                if (fieldName.equals("required")) {
                    required = nodeBooleanValue(fieldNode);
                } else if (fieldName.equals("path")) {
                    path = fieldNode.toString();
                }
            }
            this.routing = new Routing(required, path);
        } else {
            this.routing = Routing.EMPTY;
        }
        if (withoutType.containsKey("_timestamp")) {
            boolean enabled = false;
            String path = null;
            String format = TimestampFieldMapper.DEFAULT_DATE_TIME_FORMAT;
            Map<String, Object> timestampNode = (Map<String, Object>) withoutType.get("_timestamp");
            for (Map.Entry<String, Object> entry : timestampNode.entrySet()) {
                String fieldName = Strings.toUnderscoreCase(entry.getKey());
                Object fieldNode = entry.getValue();
                if (fieldName.equals("enabled")) {
                    enabled = nodeBooleanValue(fieldNode);
                } else if (fieldName.equals("path")) {
                    path = fieldNode.toString();
                } else if (fieldName.equals("format")) {
                    format = fieldNode.toString();
                }
            }
            this.timestamp = new Timestamp(enabled, path, format);
        } else {
            this.timestamp = Timestamp.EMPTY;
        }
        this.tsDateTimeFormatter = Joda.forPattern(timestamp.format());
    }

    MappingMetaData(String type, CompressedString source, Routing routing, Timestamp timestamp) {
        this.type = type;
        this.source = source;
        this.routing = routing;
        this.timestamp = timestamp;
        this.tsDateTimeFormatter = Joda.forPattern(timestamp.format());
    }

    public String type() {
        return this.type;
    }

    public CompressedString source() {
        return this.source;
    }

    public Routing routing() {
        return this.routing;
    }

    public Timestamp timestamp() {
        return this.timestamp;
    }

    public FormatDateTimeFormatter tsDateTimeFormatter() {
        return this.tsDateTimeFormatter;
    }

    public Tuple<String, String> parseRoutingAndTimestamp(XContentParser parser,
                                                         boolean shouldParseRouting,
                                                         boolean shouldParseTimestamp) throws IOException {
        return parseRoutingAndTimestamp(parser, 0, 0, null, null, shouldParseRouting, shouldParseTimestamp);
    }

    private Tuple<String, String> parseRoutingAndTimestamp(XContentParser parser,
                                                          int locationRouting,
                                                          int locationTimestamp,
                                                          @Nullable String routingValue,
                                                          @Nullable String timestampValue,
                                                          boolean shouldParseRouting,
                                                          boolean shouldParseTimestamp) throws IOException {
        XContentParser.Token t = parser.currentToken();
        if (t == null) {
            t = parser.nextToken();
        }
        if (t == XContentParser.Token.START_OBJECT) {
            t = parser.nextToken();
        }
        String routingPart = shouldParseRouting ? routing().pathElements()[locationRouting] : null;
        String timestampPart = shouldParseTimestamp ? timestamp().pathElements()[locationTimestamp] : null;

        for (; t == XContentParser.Token.FIELD_NAME; t = parser.nextToken()) {
            // Must point to field name
            String fieldName = parser.currentName();
            // And then the value...
            t = parser.nextToken();

            boolean incLocationRouting = false;
            boolean incLocationTimestamp = false;
            if (shouldParseRouting && routingPart.equals(fieldName)) {
                if (locationRouting + 1 == routing.pathElements().length) {
                    routingValue = parser.textOrNull();
                    shouldParseRouting = false;
                } else {
                    incLocationRouting = true;
                }
            }
            if (shouldParseTimestamp && timestampPart.equals(fieldName)) {
                if (locationTimestamp + 1 == timestamp.pathElements().length) {
                    timestampValue = parser.textOrNull();
                    shouldParseTimestamp = false;
                } else {
                    incLocationTimestamp = true;
                }
            }

            if (incLocationRouting || incLocationTimestamp) {
                if (t == XContentParser.Token.START_OBJECT) {
                    locationRouting += incLocationRouting ? 1 : 0;
                    locationTimestamp += incLocationTimestamp ? 1 : 0;
                    Tuple<String, String> result = parseRoutingAndTimestamp(parser, locationRouting, locationTimestamp, routingValue, timestampValue,
                            shouldParseRouting, shouldParseTimestamp);
                    routingValue = result.v1();
                    timestampValue = result.v2();
                    if (incLocationRouting) {
                        if (routingValue != null) {
                            shouldParseRouting = false;
                        } else {
                            locationRouting--;
                        }
                    }
                    if (incLocationTimestamp) {
                        if (timestampValue != null) {
                            shouldParseTimestamp = false;
                        } else {
                            locationTimestamp--;
                        }
                    }
                }
            } else {
                parser.skipChildren();
            }

            if (!shouldParseRouting && !shouldParseTimestamp) {
                return Tuple.create(routingValue, timestampValue);
            }
        }

        return Tuple.create(routingValue, timestampValue);
    }

    public static void writeTo(MappingMetaData mappingMd, StreamOutput out) throws IOException {
        out.writeUTF(mappingMd.type());
        mappingMd.source().writeTo(out);
        // routing
        out.writeBoolean(mappingMd.routing().required());
        if (mappingMd.routing().hasPath()) {
            out.writeBoolean(true);
            out.writeUTF(mappingMd.routing().path());
        } else {
            out.writeBoolean(false);
        }
        // timestamp
        out.writeBoolean(mappingMd.timestamp().enabled());
        if (mappingMd.timestamp().hasPath()) {
            out.writeBoolean(true);
            out.writeUTF(mappingMd.timestamp().path());
        } else {
            out.writeBoolean(false);
        }
        out.writeUTF(mappingMd.timestamp().format());
    }

    public static MappingMetaData readFrom(StreamInput in) throws IOException {
        String type = in.readUTF();
        CompressedString source = CompressedString.readCompressedString(in);
        // routing
        Routing routing = new Routing(in.readBoolean(), in.readBoolean() ? in.readUTF() : null);
        // timestamp
        Timestamp timestamp = new Timestamp(in.readBoolean(), in.readBoolean() ? in.readUTF() : null, in.readUTF());
        return new MappingMetaData(type, source, routing, timestamp);
    }
}
