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
import org.elasticsearch.common.xcontent.ObjectParser;

import java.util.List;


/**
 * Represents a single log line in a json format.
 * Parsing log lines with this class confirms the json format of logs
 */
public class JsonLogLine {
    public static final ObjectParser<JsonLogLine, Void> PARSER = createParser(true);

    private String type;
    private String timestamp;
    private String level;
    private String component;
    private String clusterName;
    private String nodeName;
    private String clusterUuid;
    private String nodeId;
    private String message;
    private List<String> stacktrace;

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("JsonLogLine{");
        sb.append("type='").append(type).append('\'');
        sb.append(", timestamp='").append(timestamp).append('\'');
        sb.append(", level='").append(level).append('\'');
        sb.append(", component='").append(component).append('\'');
        sb.append(", clusterName='").append(clusterName).append('\'');
        sb.append(", nodeName='").append(nodeName).append('\'');
        sb.append(", clusterUuid='").append(clusterUuid).append('\'');
        sb.append(", nodeId='").append(nodeId).append('\'');
        sb.append(", message='").append(message).append('\'');
        sb.append(", stacktrace=").append(stacktrace);
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

    public String component() {
        return component;
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

    public List<String> stacktrace() {
        return stacktrace;
    }

    public void setType(String type) {
        this.type = type;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public void setLevel(String level) {
        this.level = level;
    }

    public void setComponent(String component) {
        this.component = component;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public void setNodeName(String nodeName) {
        this.nodeName = nodeName;
    }

    public void setClusterUuid(String clusterUuid) {
        this.clusterUuid = clusterUuid;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public void setStacktrace(List<String> stacktrace) {
        this.stacktrace = stacktrace;
    }

    private static ObjectParser<JsonLogLine, Void> createParser(boolean ignoreUnknownFields) {
        ObjectParser<JsonLogLine, Void> parser = new ObjectParser<>("search_template", ignoreUnknownFields, JsonLogLine::new);
        parser.declareString(JsonLogLine::setType, new ParseField("type"));
        parser.declareString(JsonLogLine::setTimestamp, new ParseField("timestamp"));
        parser.declareString(JsonLogLine::setLevel, new ParseField("level"));
        parser.declareString(JsonLogLine::setComponent, new ParseField("component"));
        parser.declareString(JsonLogLine::setClusterName, new ParseField("cluster.name"));
        parser.declareString(JsonLogLine::setNodeName, new ParseField("node.name"));
        parser.declareString(JsonLogLine::setClusterUuid, new ParseField("cluster.uuid"));
        parser.declareString(JsonLogLine::setNodeId, new ParseField("node.id"));
        parser.declareString(JsonLogLine::setMessage, new ParseField("message"));
        parser.declareStringArray(JsonLogLine::setStacktrace, new ParseField("stacktrace"));

        return parser;
    }
}
