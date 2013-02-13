/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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
import org.elasticsearch.Version;
import org.elasticsearch.action.support.nodes.NodeOperationResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.http.HttpInfo;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.monitor.network.NetworkInfo;
import org.elasticsearch.monitor.os.OsInfo;
import org.elasticsearch.monitor.process.ProcessInfo;
import org.elasticsearch.threadpool.ThreadPoolInfo;
import org.elasticsearch.transport.TransportInfo;

import java.io.IOException;
import java.util.Map;

/**
 * Node information (static, does not change over time).
 */
public class NodeInfo extends NodeOperationResponse {

    @Nullable
    private ImmutableMap<String, String> serviceAttributes;

    @Nullable
    private String hostname;

    private Version version;

    @Nullable
    private Settings settings;

    @Nullable
    private OsInfo os;

    @Nullable
    private ProcessInfo process;

    @Nullable
    private JvmInfo jvm;

    @Nullable
    private ThreadPoolInfo threadPool;

    @Nullable
    private NetworkInfo network;

    @Nullable
    private TransportInfo transport;

    @Nullable
    private HttpInfo http;

    NodeInfo() {
    }

    public NodeInfo(@Nullable String hostname, Version version, DiscoveryNode node, @Nullable ImmutableMap<String, String> serviceAttributes, @Nullable Settings settings,
                    @Nullable OsInfo os, @Nullable ProcessInfo process, @Nullable JvmInfo jvm, @Nullable ThreadPoolInfo threadPool, @Nullable NetworkInfo network,
                    @Nullable TransportInfo transport, @Nullable HttpInfo http) {
        super(node);
        this.hostname = hostname;
        this.version = version;
        this.serviceAttributes = serviceAttributes;
        this.settings = settings;
        this.os = os;
        this.process = process;
        this.jvm = jvm;
        this.threadPool = threadPool;
        this.network = network;
        this.transport = transport;
        this.http = http;
    }

    /**
     * System's hostname. <code>null</code> in case of UnknownHostException
     */
    @Nullable
    public String hostname() {
        return this.hostname;
    }

    /**
     * System's hostname. <code>null</code> in case of UnknownHostException
     */
    @Nullable
    public String getHostname() {
        return hostname();
    }

    /**
     * The current ES version
     */
    public Version version() {
        return version;
    }

    /**
     * The current ES version
     */
    public Version getVersion() {
        return version();
    }

    /**
     * The service attributes of the node.
     */
    @Nullable
    public ImmutableMap<String, String> serviceAttributes() {
        return this.serviceAttributes;
    }

    /**
     * The attributes of the node.
     */
    @Nullable
    public ImmutableMap<String, String> getServiceAttributes() {
        return serviceAttributes();
    }

    /**
     * The settings of the node.
     */
    @Nullable
    public Settings settings() {
        return this.settings;
    }

    /**
     * The settings of the node.
     */
    @Nullable
    public Settings getSettings() {
        return settings();
    }

    /**
     * Operating System level information.
     */
    @Nullable
    public OsInfo os() {
        return this.os;
    }

    /**
     * Operating System level information.
     */
    @Nullable
    public OsInfo getOs() {
        return os();
    }

    /**
     * Process level information.
     */
    @Nullable
    public ProcessInfo process() {
        return process;
    }

    /**
     * Process level information.
     */
    @Nullable
    public ProcessInfo getProcess() {
        return process();
    }

    /**
     * JVM level information.
     */
    @Nullable
    public JvmInfo jvm() {
        return jvm;
    }

    /**
     * JVM level information.
     */
    @Nullable
    public JvmInfo getJvm() {
        return jvm();
    }

    @Nullable
    public ThreadPoolInfo threadPool() {
        return this.threadPool;
    }

    @Nullable
    public ThreadPoolInfo getThreadPool() {
        return threadPool();
    }

    /**
     * Network level information.
     */
    @Nullable
    public NetworkInfo network() {
        return network;
    }

    /**
     * Network level information.
     */
    @Nullable
    public NetworkInfo getNetwork() {
        return network();
    }

    @Nullable
    public TransportInfo transport() {
        return transport;
    }

    @Nullable
    public TransportInfo getTransport() {
        return transport();
    }

    @Nullable
    public HttpInfo http() {
        return http;
    }

    @Nullable
    public HttpInfo getHttp() {
        return http();
    }

    public static NodeInfo readNodeInfo(StreamInput in) throws IOException {
        NodeInfo nodeInfo = new NodeInfo();
        nodeInfo.readFrom(in);
        return nodeInfo;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        if (in.readBoolean()) {
            hostname = in.readString();
        }
        version = Version.readVersion(in);
        if (in.readBoolean()) {
            ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
            int size = in.readVInt();
            for (int i = 0; i < size; i++) {
                builder.put(in.readString(), in.readString());
            }
            serviceAttributes = builder.build();
        }
        if (in.readBoolean()) {
            settings = ImmutableSettings.readSettingsFromStream(in);
        }
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
            threadPool = ThreadPoolInfo.readThreadPoolInfo(in);
        }
        if (in.readBoolean()) {
            network = NetworkInfo.readNetworkInfo(in);
        }
        if (in.readBoolean()) {
            transport = TransportInfo.readTransportInfo(in);
        }
        if (in.readBoolean()) {
            http = HttpInfo.readHttpInfo(in);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (hostname == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeString(hostname);
        }
        out.writeVInt(version.id);
        if (serviceAttributes() == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeVInt(serviceAttributes.size());
            for (Map.Entry<String, String> entry : serviceAttributes.entrySet()) {
                out.writeString(entry.getKey());
                out.writeString(entry.getValue());
            }
        }
        if (settings == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            ImmutableSettings.writeSettingsToStream(settings, out);
        }
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
        if (threadPool == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            threadPool.writeTo(out);
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
        if (http == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            http.writeTo(out);
        }
    }
}
