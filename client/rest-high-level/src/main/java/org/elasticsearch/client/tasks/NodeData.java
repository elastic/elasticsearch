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
package org.elasticsearch.client.tasks;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

class NodeData {

    private String nodeId;
    private String name;
    private String transportAddress;
    private String host;
    private String ip;
    private final List<String> roles = new ArrayList<>();
    private final Map<String,String> attributes = new HashMap<>();
    private final List<TaskInfo> tasks = new ArrayList<>();

    NodeData(String nodeId) {
        this.nodeId = nodeId;
    }

    void setName(String name) {
        this.name = name;
    }

    public void setAttributes(Map<String, String> attributes) {
        if(attributes!=null){
            this.attributes.putAll(attributes);
        }
    }

    void setTransportAddress(String transportAddress) {
        this.transportAddress = transportAddress;
    }

    void setHost(String host) {
        this.host = host;
    }

    void setIp(String ip) {
        this.ip = ip;
    }

    void setRoles(List<String> roles) {
        if(roles!=null){
            this.roles.addAll(roles);
        }
    }

    public String getNodeId() {
        return nodeId;
    }

    public String getName() {
        return name;
    }

    public String getTransportAddress() {
        return transportAddress;
    }

    public String getHost() {
        return host;
    }

    public String getIp() {
        return ip;
    }

    public List<String> getRoles() {
        return roles;
    }

    public Map<String, String> getAttributes() {
        return attributes;
    }

    public List<TaskInfo> getTasks() {
        return tasks;
    }

    void setTasks(List<TaskInfo> tasks) {
        if(tasks!=null){
            this.tasks.addAll(tasks);
        }
    }

    @Override
    public String toString() {
        return "NodeData{" +
            "nodeId='" + nodeId + '\'' +
            ", name='" + name + '\'' +
            ", transportAddress='" + transportAddress + '\'' +
            ", host='" + host + '\'' +
            ", ip='" + ip + '\'' +
            ", roles=" + roles +
            ", attributes=" + attributes +
            '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof NodeData)) return false;
        NodeData nodeData = (NodeData) o;
        return Objects.equals(getNodeId(), nodeData.getNodeId()) &&
            Objects.equals(getName(), nodeData.getName()) &&
            Objects.equals(getTransportAddress(), nodeData.getTransportAddress()) &&
            Objects.equals(getHost(), nodeData.getHost()) &&
            Objects.equals(getIp(), nodeData.getIp()) &&
            Objects.equals(getRoles(), nodeData.getRoles()) &&
            Objects.equals(getAttributes(), nodeData.getAttributes()) &&
            Objects.equals(getTasks(), nodeData.getTasks());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getNodeId(), getName(), getTransportAddress(), getHost(), getIp(), getRoles(), getAttributes(), getTasks());
    }

    public static final ObjectParser.NamedObjectParser<NodeData, Void> PARSER;

    static {
        ObjectParser<NodeData, Void> parser = new ObjectParser<>("nodes");
        parser.declareString(NodeData::setName, new ParseField("name"));
        parser.declareString(NodeData::setTransportAddress, new ParseField("transport_address"));
        parser.declareString(NodeData::setHost, new ParseField("host"));
        parser.declareString(NodeData::setIp, new ParseField("ip"));
        parser.declareStringArray(NodeData::setRoles, new ParseField("roles"));
        parser.declareField(NodeData::setAttributes,
           (p, c) -> p.mapStrings(),
           new ParseField("attributes"),
           ObjectParser.ValueType.OBJECT);
        parser.declareNamedObjects(NodeData::setTasks, TaskInfo.PARSER, new ParseField("tasks"));
        PARSER = (XContentParser p, Void v, String nodeId) -> parser.parse(p, new NodeData(nodeId), null);
    }
}
