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

package org.elasticsearch.action.admin.cluster.node.info;

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.action.support.nodes.NodeOperationResponse;
import org.elasticsearch.cluster.node.Node;
import org.elasticsearch.util.settings.ImmutableSettings;
import org.elasticsearch.util.settings.Settings;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

/**
 * @author kimchy (Shay Banon)
 */
public class NodeInfo extends NodeOperationResponse {

    private ImmutableMap<String, String> attributes;

    private Settings settings;

    NodeInfo() {
    }

    public NodeInfo(Node node, Map<String, String> attributes, Settings settings) {
        this(node, ImmutableMap.copyOf(attributes), settings);
    }

    public NodeInfo(Node node, ImmutableMap<String, String> attributes, Settings settings) {
        super(node);
        this.attributes = attributes;
        this.settings = settings;
    }

    public ImmutableMap<String, String> attributes() {
        return this.attributes;
    }

    public Settings settings() {
        return this.settings;
    }

    public static NodeInfo readNodeInfo(DataInput in) throws ClassNotFoundException, IOException {
        NodeInfo nodeInfo = new NodeInfo();
        nodeInfo.readFrom(in);
        return nodeInfo;
    }

    @Override public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
        super.readFrom(in);
        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            builder.put(in.readUTF(), in.readUTF());
        }
        attributes = builder.build();
        settings = ImmutableSettings.readSettingsFromStream(in);
    }

    @Override public void writeTo(DataOutput out) throws IOException {
        super.writeTo(out);
        out.writeInt(attributes.size());
        for (Map.Entry<String, String> entry : attributes.entrySet()) {
            out.writeUTF(entry.getKey());
            out.writeUTF(entry.getValue());
        }
        ImmutableSettings.writeSettingsToStream(settings, out);
    }
}
