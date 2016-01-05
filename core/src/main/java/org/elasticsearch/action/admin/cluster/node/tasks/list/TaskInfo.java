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

package org.elasticsearch.action.admin.cluster.node.tasks.list;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Information about a currently running task.
 * <p>
 * Tasks are used for communication with transport actions. As a result, they can contain callback
 * references as well as mutable state. That makes it impractical to send tasks over transport channels
 * and use in APIs. Instead, immutable and streamable TaskInfo objects are used to represent
 * snapshot information about currently running tasks.
 */
public class TaskInfo implements Writeable<TaskInfo>, ToXContent {

    private final DiscoveryNode node;

    private final long id;

    private final String type;

    private final String action;

    private final String description;

    private final String parentNode;

    private final long parentId;

    public TaskInfo(DiscoveryNode node, long id, String type, String action, String description) {
        this(node, id, type, action, description, null, -1L);
    }

    public TaskInfo(DiscoveryNode node, long id, String type, String action, String description, String parentNode, long parentId) {
        this.node = node;
        this.id = id;
        this.type = type;
        this.action = action;
        this.description = description;
        this.parentNode = parentNode;
        this.parentId = parentId;
    }

    public TaskInfo(StreamInput in) throws IOException {
        node = DiscoveryNode.readNode(in);
        id = in.readLong();
        type = in.readString();
        action = in.readString();
        description = in.readOptionalString();
        parentNode = in.readOptionalString();
        parentId = in.readLong();
    }

    public DiscoveryNode getNode() {
        return node;
    }

    public long getId() {
        return id;
    }

    public String getType() {
        return type;
    }

    public String getAction() {
        return action;
    }

    public String getDescription() {
        return description;
    }

    public String getParentNode() {
        return parentNode;
    }

    public long getParentId() {
        return parentId;
    }

    @Override
    public TaskInfo readFrom(StreamInput in) throws IOException {
        return new TaskInfo(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        node.writeTo(out);
        out.writeLong(id);
        out.writeString(type);
        out.writeString(action);
        out.writeOptionalString(description);
        out.writeOptionalString(parentNode);
        out.writeLong(parentId);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("node", node.getId());
        builder.field("id", id);
        builder.field("type", type);
        builder.field("action", action);
        if (description != null) {
            builder.field("description", description);
        }
        if (parentNode != null) {
            builder.field("parent_node", parentNode);
            builder.field("parent_id", parentId);
        }
        builder.endObject();
        return builder;
    }
}
