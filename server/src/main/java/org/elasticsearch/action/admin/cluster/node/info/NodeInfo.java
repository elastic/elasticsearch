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
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.http.HttpInfo;
import org.elasticsearch.ingest.IngestInfo;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.monitor.os.OsInfo;
import org.elasticsearch.monitor.process.ProcessInfo;
import org.elasticsearch.threadpool.ThreadPoolInfo;
import org.elasticsearch.transport.TransportInfo;

import java.io.IOException;

/**
 * Node information (static, does not change over time).
 */
public class NodeInfo extends BaseNodeResponse {

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

    @Nullable
    private ByteSizeValue totalIndexingBuffer;

    public NodeInfo(StreamInput in) throws IOException {
        super(in);
        version = Version.readVersion(in);
        build = Build.readBuild(in);
        if (in.readBoolean()) {
            totalIndexingBuffer = new ByteSizeValue(in.readLong());
        } else {
            totalIndexingBuffer = null;
        }
        if (in.readBoolean()) {
            settings = Settings.readSettingsFromStream(in);
        }
        os = in.readOptionalWriteable(OsInfo::new);
        process = in.readOptionalWriteable(ProcessInfo::new);
        jvm = in.readOptionalWriteable(JvmInfo::new);
        threadPool = in.readOptionalWriteable(ThreadPoolInfo::new);
        transport = in.readOptionalWriteable(TransportInfo::new);
        http = in.readOptionalWriteable(HttpInfo::new);
        plugins = in.readOptionalWriteable(PluginsAndModules::new);
        ingest = in.readOptionalWriteable(IngestInfo::new);
    }

    public NodeInfo(Version version, Build build, DiscoveryNode node, @Nullable Settings settings,
                    @Nullable OsInfo os, @Nullable ProcessInfo process, @Nullable JvmInfo jvm, @Nullable ThreadPoolInfo threadPool,
                    @Nullable TransportInfo transport, @Nullable HttpInfo http, @Nullable PluginsAndModules plugins,
                    @Nullable IngestInfo ingest, @Nullable ByteSizeValue totalIndexingBuffer) {
        super(node);
        this.version = version;
        this.build = build;
        this.settings = settings;
        this.os = os;
        this.process = process;
        this.jvm = jvm;
        this.threadPool = threadPool;
        this.transport = transport;
        this.http = http;
        this.plugins = plugins;
        this.ingest = ingest;
        this.totalIndexingBuffer = totalIndexingBuffer;
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

    @Nullable
    public ByteSizeValue getTotalIndexingBuffer() {
        return totalIndexingBuffer;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(version.id);
        Build.writeBuild(build, out);
        if (totalIndexingBuffer == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeLong(totalIndexingBuffer.getBytes());
        }
        if (settings == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            Settings.writeSettingsToStream(settings, out);
        }
        out.writeOptionalWriteable(os);
        out.writeOptionalWriteable(process);
        out.writeOptionalWriteable(jvm);
        out.writeOptionalWriteable(threadPool);
        out.writeOptionalWriteable(transport);
        out.writeOptionalWriteable(http);
        out.writeOptionalWriteable(plugins);
        out.writeOptionalWriteable(ingest);
    }
}
