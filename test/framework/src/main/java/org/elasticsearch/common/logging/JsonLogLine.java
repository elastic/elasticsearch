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

package org.elasticsearch.common.logging;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;

import java.util.List;

public class JsonLogLine {
    public static final ConstructingObjectParser<JsonLogLine, Void> PARSER = createParser(false);
    private String type;
    private String timestamp;
    private String level;
    private String clazz;
    private String clusterName;
    private String nodeName;
    private String clusterUuid;
    private String nodeId;
    private String message;
    private List<String> exceptions;

    private JsonLogLine(String type, String timestamp, String level, String clazz, String clusterName,
                        String nodeName, String clusterUuid, String nodeId,
                        String message,
                        List<String> exceptions) {
        this.type = type;
        this.timestamp = timestamp;
        this.level = level;
        this.clazz = clazz;
        this.clusterName = clusterName;
        this.nodeName = nodeName;
        this.clusterUuid = clusterUuid;
        this.nodeId = nodeId;
        this.message = message;
        this.exceptions = exceptions;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("JsonLogLine{");
        sb.append("type='").append(type).append('\'');
        sb.append(", timestamp='").append(timestamp).append('\'');
        sb.append(", level='").append(level).append('\'');
        sb.append(", clazz='").append(clazz).append('\'');
        sb.append(", clusterName='").append(clusterName).append('\'');
        sb.append(", nodeName='").append(nodeName).append('\'');
        sb.append(", clusterUuid='").append(clusterUuid).append('\'');
        sb.append(", nodeId='").append(nodeId).append('\'');
        sb.append(", message='").append(message).append('\'');
        sb.append(", exceptions=").append(exceptions);
        sb.append('}');
        return sb.toString();
    }

    public String type() {
        return type;
    }

    public String timestamp() {
        return timestamp;
    }

    public String level() {
        return level;
    }

    public String clazz() {
        return clazz;
    }

    public String clusterName() {
        return clusterName;
    }

    public String nodeName() {
        return nodeName;
    }

    public String clusterUuid() {
        return clusterUuid;
    }

    public String nodeId() {
        return nodeId;
    }

    public String message() {
        return message;
    }

    public List<String> exceptions() {
        return exceptions;
    }

    @SuppressWarnings("unchecked")
    private static ConstructingObjectParser<JsonLogLine, Void> createParser(boolean ignoreUnknownFields) {
        ConstructingObjectParser<JsonLogLine, Void> parser = new ConstructingObjectParser<>("jsong_log_line_parser", ignoreUnknownFields,
            a -> JsonLogLine.builder()
                .withType((String) a[0])
                .withTimestamp((String) a[1])
                .withLevel((String) a[2])
                .withClazz((String) a[3])
                .withClusterName((String) a[4])
                .withNodeName((String) a[5])
                .withClusterUuid((String) a[6])
                .withNodeId((String) a[7])
                .withMessage((String) a[8])
                .withExceptions((List<String>) a[9])
                .build()
        );

        parser.declareString(ConstructingObjectParser.constructorArg(), new ParseField("type"));
        parser.declareString(ConstructingObjectParser.constructorArg(), new ParseField("timestamp"));
        parser.declareString(ConstructingObjectParser.constructorArg(), new ParseField("level"));
        parser.declareString(ConstructingObjectParser.constructorArg(), new ParseField("class"));
        parser.declareString(ConstructingObjectParser.constructorArg(), new ParseField("cluster.name"));
        parser.declareString(ConstructingObjectParser.constructorArg(), new ParseField("node.name"));
        parser.declareString(ConstructingObjectParser.optionalConstructorArg(), new ParseField("cluster.uuid"));
        parser.declareString(ConstructingObjectParser.optionalConstructorArg(), new ParseField("node.id"));
        parser.declareString(ConstructingObjectParser.constructorArg(), new ParseField("message"));
        parser.declareStringArray(ConstructingObjectParser.optionalConstructorArg(), new ParseField("exception"));


        return parser;
    }

    public static Builder builder() {
        return new Builder();
    }

    static class Builder {
        String type;
        String timestamp;
        String level;
        String clazz;
        String clusterName;
        String nodeName;
        String clusterUuid;
        String nodeId;
        String message;
        List<String> exception;

        public Builder withType(String type) {
            this.type = type;
            return this;
        }

        public Builder withTimestamp(String timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        public Builder withLevel(String level) {
            this.level = level;
            return this;
        }

        public Builder withClazz(String clazz) {
            this.clazz = clazz;
            return this;
        }

        public Builder withClusterName(String clusterName) {
            this.clusterName = clusterName;
            return this;
        }

        public Builder withNodeName(String nodeName) {
            this.nodeName = nodeName;
            return this;
        }

        public Builder withClusterUuid(String clusterUuid) {
            this.clusterUuid = clusterUuid;
            return this;
        }

        public Builder withNodeId(String nodeId) {
            this.nodeId = nodeId;
            return this;
        }

        public Builder withMessage(String message) {
            this.message = message;
            return this;
        }

        public Builder withExceptions(List<String> exception) {
            this.exception = exception;
            return this;
        }

        public JsonLogLine build() {
            return new JsonLogLine(type, timestamp, level, clazz, clusterName,
                nodeName, clusterUuid, nodeId, message, exception);
        }
    }
}
