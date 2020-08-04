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
import org.elasticsearch.node.ReportingService;
import org.elasticsearch.search.aggregations.support.AggregationInfo;
import org.elasticsearch.threadpool.ThreadPoolInfo;
import org.elasticsearch.transport.TransportInfo;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Node information (static, does not change over time).
 */
public class NodeInfo extends BaseNodeResponse {

    private Version version;
    private Build build;

    @Nullable
    private Settings settings;

    /**
     * Do not expose this map to other classes. For type safety, use {@link #getInfo(Class)}
     * to retrieve items from this map and {@link #addInfoIfNonNull(Class, ReportingService.Info)}
     * to retrieve items from it.
     */
    private Map<Class<? extends ReportingService.Info>, ReportingService.Info> infoMap = new HashMap<>();

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
        addInfoIfNonNull(OsInfo.class, in.readOptionalWriteable(OsInfo::new));
        addInfoIfNonNull(ProcessInfo.class, in.readOptionalWriteable(ProcessInfo::new));
        addInfoIfNonNull(JvmInfo.class, in.readOptionalWriteable(JvmInfo::new));
        addInfoIfNonNull(ThreadPoolInfo.class, in.readOptionalWriteable(ThreadPoolInfo::new));
        addInfoIfNonNull(TransportInfo.class, in.readOptionalWriteable(TransportInfo::new));
        addInfoIfNonNull(HttpInfo.class, in.readOptionalWriteable(HttpInfo::new));
        addInfoIfNonNull(PluginsAndModules.class, in.readOptionalWriteable(PluginsAndModules::new));
        addInfoIfNonNull(IngestInfo.class, in.readOptionalWriteable(IngestInfo::new));
        if (in.getVersion().onOrAfter(Version.V_7_10_0)) {
            addInfoIfNonNull(AggregationInfo.class, in.readOptionalWriteable(AggregationInfo::new));
        }
    }

    public NodeInfo(Version version, Build build, DiscoveryNode node, @Nullable Settings settings,
                    @Nullable OsInfo os, @Nullable ProcessInfo process, @Nullable JvmInfo jvm, @Nullable ThreadPoolInfo threadPool,
                    @Nullable TransportInfo transport, @Nullable HttpInfo http, @Nullable PluginsAndModules plugins,
                    @Nullable IngestInfo ingest, @Nullable AggregationInfo aggsInfo, @Nullable ByteSizeValue totalIndexingBuffer) {
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
        addInfoIfNonNull(AggregationInfo.class, aggsInfo);
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
     * Get a particular info object, e.g. {@link JvmInfo} or {@link OsInfo}. This
     * generic method handles all casting in order to spare client classes the
     * work of explicit casts. This {@link NodeInfo} class guarantees type
     * safety for these stored info blocks.
     *
     * @param clazz Class for retrieval.
     * @param <T>   Specific subtype of ReportingService.Info to retrieve.
     * @return      An object of type T.
     */
    public <T extends ReportingService.Info> T getInfo(Class<T> clazz) {
        return clazz.cast(infoMap.get(clazz));
    }

    @Nullable
    public ByteSizeValue getTotalIndexingBuffer() {
        return totalIndexingBuffer;
    }

    /**
     * Add a value to the map of information blocks. This method guarantees the
     * type safety of the storage of heterogeneous types of reporting service information.
     */
    private <T extends ReportingService.Info> void addInfoIfNonNull(Class<T> clazz, T info) {
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
        out.writeOptionalWriteable(getInfo(OsInfo.class));
        out.writeOptionalWriteable(getInfo(ProcessInfo.class));
        out.writeOptionalWriteable(getInfo(JvmInfo.class));
        out.writeOptionalWriteable(getInfo(ThreadPoolInfo.class));
        out.writeOptionalWriteable(getInfo(TransportInfo.class));
        out.writeOptionalWriteable(getInfo(HttpInfo.class));
        out.writeOptionalWriteable(getInfo(PluginsAndModules.class));
        out.writeOptionalWriteable(getInfo(IngestInfo.class));
        if (out.getVersion().onOrAfter(Version.V_7_10_0)) {
            out.writeOptionalWriteable(getInfo(AggregationInfo.class));
        }
    }
}
