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

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedString;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.DocumentMapper;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.*;

/**
 * @author kimchy (shay.banon)
 */
public class MappingMetaData {

    private static ESLogger logger = ESLoggerFactory.getLogger(MappingMetaData.class.getName());

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

    private final String type;

    private final CompressedString source;

    private final Routing routing;

    public MappingMetaData(DocumentMapper docMapper) {
        this.type = docMapper.type();
        this.source = docMapper.mappingSource();
        this.routing = new Routing(docMapper.routingFieldMapper().required(), docMapper.routingFieldMapper().path());
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
    }

    MappingMetaData(String type, CompressedString source, Routing routing) {
        this.type = type;
        this.source = source;
        this.routing = routing;
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

    public String parseRouting(XContentParser parser) throws IOException {
        return parseRouting(parser, 0);
    }

    private String parseRouting(XContentParser parser, int location) throws IOException {
        XContentParser.Token t = parser.currentToken();
        if (t == null) {
            t = parser.nextToken();
        }
        if (t == XContentParser.Token.START_OBJECT) {
            t = parser.nextToken();
        }
        String routingPart = routing().pathElements()[location];

        for (; t == XContentParser.Token.FIELD_NAME; t = parser.nextToken()) {
            // Must point to field name
            String fieldName = parser.currentName();
            // And then the value...
            t = parser.nextToken();
            if (routingPart.equals(fieldName)) {
                location++;
                if (location == routing.pathElements().length) {
                    return parser.textOrNull();
                }
                if (t == XContentParser.Token.START_OBJECT) {
                    return parseRouting(parser, location);
                }
            } else {
                parser.skipChildren();
            }
        }
        return null;
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
    }

    public static MappingMetaData readFrom(StreamInput in) throws IOException {
        String type = in.readUTF();
        CompressedString source = CompressedString.readCompressedString(in);
        // routing
        Routing routing = new Routing(in.readBoolean(), in.readBoolean() ? in.readUTF() : null);
        return new MappingMetaData(type, source, routing);
    }
}
