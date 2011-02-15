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

import org.elasticsearch.action.support.nodes.NodeOperationResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.monitor.network.NetworkInfo;
import org.elasticsearch.monitor.os.OsInfo;
import org.elasticsearch.monitor.process.ProcessInfo;
import org.elasticsearch.transport.TransportInfo;

import java.io.IOException;
import java.util.Map;

/**
 * Node information (static, does not change over time).
 *
 * @author kimchy (shay.banon)
 */
public class NodeInfo extends NodeOperationResponse {

    private ImmutableMap<String, String> attributes;

    private Settings settings;

    private OsInfo os;

    private ProcessInfo process;

    private JvmInfo jvm;

    private NetworkInfo network;

    private TransportInfo transport;

    NodeInfo() {
    }

    public NodeInfo(DiscoveryNode node, ImmutableMap<String, String> attributes, Settings settings,
                    OsInfo os, ProcessInfo process, JvmInfo jvm, NetworkInfo network,
                    TransportInfo transport) {
        super(node);
        this.attributes = attributes;
        this.settings = settings;
        this.os = os;
        this.process = process;
        this.jvm = jvm;
        this.network = network;
        this.transport = transport;
    }

    /**
     * The attributes of the node.
     */
    public ImmutableMap<String, String> attributes() {
        return this.attributes;
    }

    /**
     * The attributes of the node.
     */
    public ImmutableMap<String, String> getAttributes() {
        return attributes();
    }

    /**
     * The settings of the node.
     */
    public Settings settings() {
        return this.settings;
    }

    /**
     * The settings of the node.
     */
    public Settings getSettings() {
        return settings();
    }

    /**
     * Operating System level information.
     */
    public OsInfo os() {
        return this.os;
    }

    /**
     * Operating System level information.
     */
    public OsInfo getOs() {
        return os();
    }

    /**
     * Process level information.
     */
    public ProcessInfo process() {
        return process;
    }

    /**
     * Process level information.
     */
    public ProcessInfo getProcess() {
        return process();
    }

    /**
     * JVM level information.
     */
    public JvmInfo jvm() {
        return jvm;
    }

    /**
     * JVM level information.
     */
    public JvmInfo getJvm() {
        return jvm();
    }

    /**
     * Network level information.
     */
    public NetworkInfo network() {
        return network;
    }

    /**
     * Network level information.
     */
    public NetworkInfo getNetwork() {
        return network();
    }

    public TransportInfo transport() {
        return transport;
    }

    public TransportInfo getTransport() {
        return transport();
    }

    public static NodeInfo readNodeInfo(StreamInput in) throws IOException {
        NodeInfo nodeInfo = new NodeInfo();
        nodeInfo.readFrom(in);
        return nodeInfo;
    }

    @Override public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        int size = in.readVInt();
        for (int i = 0; i < size; i++) {
            builder.put(in.readUTF(), in.readUTF());
        }
        attributes = builder.build();
        settings = ImmutableSettings.readSettingsFromStream(in);
        if (in.readBoolean()) {
            os = OsInfo.readOsInfo(in);
        }
        if (in.readBoolean()) {
            process = ProcessInfo.readProcessInfo(in);
        }
        if (in.readBoolean()) {
            jvm = JvmInfo.readJvmInfo(in);
        }
        if (in.readBoolean()) {
            network = NetworkInfo.readNetworkInfo(in);
        }
        if (in.readBoolean()) {
            transport = TransportInfo.readTransportInfo(in);
        }
    }

    @Override public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(attributes.size());
        for (Map.Entry<String, String> entry : attributes.entrySet()) {
            out.writeUTF(entry.getKey());
            out.writeUTF(entry.getValue());
        }
        ImmutableSettings.writeSettingsToStream(settings, out);
        if (os == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            os.writeTo(out);
        }
        if (process == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            process.writeTo(out);
        }
        if (jvm == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            jvm.writeTo(out);
        }
        if (network == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            network.writeTo(out);
        }
        if (transport == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            transport.writeTo(out);
        }
    }
}
