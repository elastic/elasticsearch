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
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.http.HttpInfo;
import org.elasticsearch.ingest.IngestInfo;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.monitor.os.OsInfo;
import org.elasticsearch.monitor.process.ProcessInfo;
import org.elasticsearch.node.ReportingService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.threadpool.ThreadPoolInfo;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportInfo;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Node information (static, does not change over time).
 */
public class NodeInfo extends BaseNodeResponse {

    // TODO: should this go into a global named registry?
    private static NamedWriteableRegistry LOCAL_REGISTRY = new NamedWriteableRegistry(
        List.of(
            new NamedWriteableRegistry.Entry(ReportingService.Info.class, "OsInfo", OsInfo::new),
            new NamedWriteableRegistry.Entry(ReportingService.Info.class, "ProcessInfo", ProcessInfo::new),
            new NamedWriteableRegistry.Entry(ReportingService.Info.class, "JvmInfo", JvmInfo::new),
            new NamedWriteableRegistry.Entry(ReportingService.Info.class, "ThreadPoolInfo", ThreadPoolInfo::new),
            new NamedWriteableRegistry.Entry(ReportingService.Info.class, "TransportInfo", TransportInfo::new),
            new NamedWriteableRegistry.Entry(ReportingService.Info.class, "HttpInfo", HttpInfo::new),
            new NamedWriteableRegistry.Entry(ReportingService.Info.class, "PluginsAndModules", PluginsAndModules::new),
            new NamedWriteableRegistry.Entry(ReportingService.Info.class, "IngestInfo", IngestInfo::new)
        )
    );

    private Version version;
    private Build build;

    @Nullable
    private Settings settings;

    // TODO: is it OK to key this map on class?
    Map<Class<? extends ReportingService.Info>, ReportingService.Info> infoMap = new HashMap<>();

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
        // TODO[wrb]: Change this to V_7_8_0
        if (in.getVersion().onOrAfter(Version.V_8_0_0)) {
            int numberOfWriteables = in.readVInt();
            StreamInput awareStream = new NamedWriteableAwareStreamInput(in, LOCAL_REGISTRY);
            for (int i = 0; i < numberOfWriteables; i++) {
                ReportingService.Info info = awareStream.readNamedWriteable(ReportingService.Info.class);
                infoMap.put(info.getClass(), info);
            }
        } else {
            addInfoIfNonNull(OsInfo.class, in.readOptionalWriteable(OsInfo::new));
            addInfoIfNonNull(ProcessInfo.class, in.readOptionalWriteable(ProcessInfo::new));
            addInfoIfNonNull(JvmInfo.class, in.readOptionalWriteable(JvmInfo::new));
            addInfoIfNonNull(ThreadPoolInfo.class, in.readOptionalWriteable(ThreadPoolInfo::new));
            addInfoIfNonNull(TransportInfo.class, in.readOptionalWriteable(TransportInfo::new));
            addInfoIfNonNull(HttpInfo.class, in.readOptionalWriteable(HttpInfo::new));
            addInfoIfNonNull(PluginsAndModules.class, in.readOptionalWriteable(PluginsAndModules::new));
            addInfoIfNonNull(IngestInfo.class, in.readOptionalWriteable(IngestInfo::new));
        }
    }

    public NodeInfo(Version version, Build build, DiscoveryNode node, @Nullable Settings settings,
                    @Nullable OsInfo os, @Nullable ProcessInfo process, @Nullable JvmInfo jvm, @Nullable ThreadPoolInfo threadPool,
                    @Nullable TransportInfo transport, @Nullable HttpInfo http, @Nullable PluginsAndModules plugins,
                    @Nullable IngestInfo ingest, @Nullable ByteSizeValue totalIndexingBuffer) {
        super(node);
        this.version = version;
        this.build = build;
        this.settings = settings;
        addInfoIfNonNull(OsInfo.class, os);
        addInfoIfNonNull(ProcessInfo.class, process);
        addInfoIfNonNull(JvmInfo.class, jvm);
        addInfoIfNonNull(ThreadPoolInfo.class, threadPool);
        addInfoIfNonNull(TransportInfo.class, transport);
        addInfoIfNonNull(HttpInfo.class, http);
        addInfoIfNonNull(PluginsAndModules.class, plugins);
        addInfoIfNonNull(IngestInfo.class, ingest);
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

    // TODO: javadoc on the utility of a method that hides casting
    public <T extends ReportingService.Info> T getInfo(Class<T> clazz) {
        return clazz.cast(infoMap.get(clazz));
    }

    /**
     * Operating System level information.
     */
    @Nullable
    public OsInfo getOs() {
        return getInfo(OsInfo.class);
    }

    /**
     * Process level information.
     */
    @Nullable
    public ProcessInfo getProcess() {
        return getInfo(ProcessInfo.class);
    }

    /**
     * JVM level information.
     */
    @Nullable
    public JvmInfo getJvm() {
        return getInfo(JvmInfo.class);
    }

    @Nullable
    public ThreadPoolInfo getThreadPool() {
        return getInfo(ThreadPoolInfo.class);
    }

    @Nullable
    public TransportInfo getTransport() {
        return getInfo(TransportInfo.class);
    }

    @Nullable
    public HttpInfo getHttp() {
        return getInfo(HttpInfo.class);
    }

    @Nullable
    public PluginsAndModules getPlugins() {
        return getInfo(PluginsAndModules.class);
    }

    @Nullable
    public IngestInfo getIngest() {
        return getInfo(IngestInfo.class);
    }

    @Nullable
    public ByteSizeValue getTotalIndexingBuffer() {
        return totalIndexingBuffer;
    }

    private void addInfoIfNonNull(Class<? extends ReportingService.Info> clazz, ReportingService.Info info) {
        if (info != null) {
            infoMap.put(clazz, info);
        }
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
        // TODO[wrb]: Change this to V_7_8_0
        if (out.getVersion().onOrAfter(Version.V_8_0_0)) {
            out.writeVInt(infoMap.size());
            // The NodesInfoResponse class controls the ordering of what goes to the user
            for (Map.Entry<Class<? extends ReportingService.Info>, ReportingService.Info> entry : infoMap.entrySet()) {
                ReportingService.Info value = entry.getValue();
                out.writeNamedWriteable(value);
            }
        } else {
            out.writeOptionalWriteable(getInfo(OsInfo.class));
            out.writeOptionalWriteable(getInfo(ProcessInfo.class));
            out.writeOptionalWriteable(getInfo(JvmInfo.class));
            out.writeOptionalWriteable(getInfo(ThreadPoolInfo.class));
            out.writeOptionalWriteable(getInfo(TransportInfo.class));
            out.writeOptionalWriteable(getInfo(HttpInfo.class));
            out.writeOptionalWriteable(getInfo(PluginsAndModules.class));
            out.writeOptionalWriteable(getInfo(IngestInfo.class));
        }
    }
}
