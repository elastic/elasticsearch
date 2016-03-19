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

package org.elasticsearch.action.admin.cluster.node.info;

import org.elasticsearch.Build;
import org.elasticsearch.Version;
import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.http.HttpInfo;
import org.elasticsearch.ingest.core.IngestInfo;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.monitor.os.OsInfo;
import org.elasticsearch.monitor.process.ProcessInfo;
import org.elasticsearch.threadpool.ThreadPoolInfo;
import org.elasticsearch.transport.TransportInfo;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.unmodifiableMap;

/**
 * Node information (static, does not change over time).
 */
public class NodeInfo extends BaseNodeResponse {
    @Nullable
    private Map<String, String> serviceAttributes;

    private Version version;
    private Build build;

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
    private TransportInfo transport;

    @Nullable
    private HttpInfo http;

    @Nullable
    private PluginsAndModules plugins;

    @Nullable
    private IngestInfo ingest;

    public NodeInfo() {
    }

    public NodeInfo(Version version, Build build, DiscoveryNode node, @Nullable Map<String, String> serviceAttributes, @Nullable Settings settings,
                    @Nullable OsInfo os, @Nullable ProcessInfo process, @Nullable JvmInfo jvm, @Nullable ThreadPoolInfo threadPool,
                    @Nullable TransportInfo transport, @Nullable HttpInfo http, @Nullable PluginsAndModules plugins, @Nullable IngestInfo ingest) {
        super(node);
        this.version = version;
        this.build = build;
        this.serviceAttributes = serviceAttributes;
        this.settings = settings;
        this.os = os;
        this.process = process;
        this.jvm = jvm;
        this.threadPool = threadPool;
        this.transport = transport;
        this.http = http;
        this.plugins = plugins;
        this.ingest = ingest;
    }

    /**
     * System's hostname. <code>null</code> in case of UnknownHostException
     */
    @Nullable
    public String getHostname() {
        return getNode().getHostName();
    }

    /**
     * The current ES version
     */
    public Version getVersion() {
        return version;
    }

    /**
     * The build version of the node.
     */
    public Build getBuild() {
        return this.build;
    }

    /**
     * The service attributes of the node.
     */
    @Nullable
    public Map<String, String> getServiceAttributes() {
        return this.serviceAttributes;
    }

    /**
     * The settings of the node.
     */
    @Nullable
    public Settings getSettings() {
        return this.settings;
    }

    /**
     * Operating System level information.
     */
    @Nullable
    public OsInfo getOs() {
        return this.os;
    }

    /**
     * Process level information.
     */
    @Nullable
    public ProcessInfo getProcess() {
        return process;
    }

    /**
     * JVM level information.
     */
    @Nullable
    public JvmInfo getJvm() {
        return jvm;
    }

    @Nullable
    public ThreadPoolInfo getThreadPool() {
        return this.threadPool;
    }

    @Nullable
    public TransportInfo getTransport() {
        return transport;
    }

    @Nullable
    public HttpInfo getHttp() {
        return http;
    }

    @Nullable
    public PluginsAndModules getPlugins() {
        return this.plugins;
    }

    @Nullable
    public IngestInfo getIngest() {
        return ingest;
    }

    public static NodeInfo readNodeInfo(StreamInput in) throws IOException {
        NodeInfo nodeInfo = new NodeInfo();
        nodeInfo.readFrom(in);
        return nodeInfo;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        version = Version.readVersion(in);
        build = Build.readBuild(in);
        if (in.readBoolean()) {
            Map<String, String> builder = new HashMap<>();
            int size = in.readVInt();
            for (int i = 0; i < size; i++) {
                builder.put(in.readString(), in.readString());
            }
            serviceAttributes = unmodifiableMap(builder);
        }
        if (in.readBoolean()) {
            settings = Settings.readSettingsFromStream(in);
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
            transport = TransportInfo.readTransportInfo(in);
        }
        if (in.readBoolean()) {
            http = HttpInfo.readHttpInfo(in);
        }
        if (in.readBoolean()) {
            plugins = new PluginsAndModules();
            plugins.readFrom(in);
        }
        if (in.readBoolean()) {
            ingest = new IngestInfo(in);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(version.id);
        Build.writeBuild(build, out);
        if (getServiceAttributes() == null) {
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
            Settings.writeSettingsToStream(settings, out);
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
        if (plugins == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            plugins.writeTo(out);
        }
        if (ingest == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            ingest.writeTo(out);
        }
    }
}
