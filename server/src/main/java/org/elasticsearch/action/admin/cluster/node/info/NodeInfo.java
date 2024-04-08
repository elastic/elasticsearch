/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.node.info;

import org.elasticsearch.Build;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.Version;
import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.http.HttpInfo;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.ingest.IngestInfo;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.monitor.os.OsInfo;
import org.elasticsearch.monitor.process.ProcessInfo;
import org.elasticsearch.node.ReportingService;
import org.elasticsearch.search.aggregations.support.AggregationInfo;
import org.elasticsearch.threadpool.ThreadPoolInfo;
import org.elasticsearch.transport.RemoteClusterServerInfo;
import org.elasticsearch.transport.TransportInfo;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Node information (static, does not change over time).
 */
public class NodeInfo extends BaseNodeResponse {

    private final String version;
    private final TransportVersion transportVersion;
    private final IndexVersion indexVersion;
    private final Map<String, Integer> componentVersions;
    private final Build build;

    @Nullable
    private final Settings settings;

    /**
     * Do not expose this map to other classes. For type safety, use {@link #getInfo(Class)}
     * to retrieve items from this map and {@link #addInfoIfNonNull(Class, ReportingService.Info)}
     * to retrieve items from it.
     */
    private final Map<Class<? extends ReportingService.Info>, ReportingService.Info> infoMap = new HashMap<>();

    @Nullable
    private final ByteSizeValue totalIndexingBuffer;

    public NodeInfo(StreamInput in) throws IOException {
        super(in);
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_12_0)) {
            version = in.readString();
            transportVersion = TransportVersion.readVersion(in);
            indexVersion = IndexVersion.readVersion(in);
        } else {
            Version legacyVersion = Version.readVersion(in);
            version = legacyVersion.toString();
            if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_8_0)) {
                transportVersion = TransportVersion.readVersion(in);
            } else {
                transportVersion = TransportVersion.fromId(legacyVersion.id);
            }
            if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_11_X)) {
                indexVersion = IndexVersion.readVersion(in);
            } else {
                indexVersion = IndexVersion.fromId(legacyVersion.id);
            }
        }
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_11_X)) {
            componentVersions = in.readImmutableMap(StreamInput::readString, StreamInput::readVInt);
        } else {
            componentVersions = Map.of();
        }
        build = Build.readBuild(in);
        if (in.readBoolean()) {
            totalIndexingBuffer = ByteSizeValue.ofBytes(in.readLong());
        } else {
            totalIndexingBuffer = null;
        }
        if (in.readBoolean()) {
            settings = Settings.readSettingsFromStream(in);
        } else {
            settings = null;
        }
        addInfoIfNonNull(OsInfo.class, in.readOptionalWriteable(OsInfo::new));
        addInfoIfNonNull(ProcessInfo.class, in.readOptionalWriteable(ProcessInfo::new));
        addInfoIfNonNull(JvmInfo.class, in.readOptionalWriteable(JvmInfo::new));
        addInfoIfNonNull(ThreadPoolInfo.class, in.readOptionalWriteable(ThreadPoolInfo::new));
        addInfoIfNonNull(TransportInfo.class, in.readOptionalWriteable(TransportInfo::new));
        addInfoIfNonNull(HttpInfo.class, in.readOptionalWriteable(HttpInfo::new));
        addInfoIfNonNull(PluginsAndModules.class, in.readOptionalWriteable(PluginsAndModules::new));
        addInfoIfNonNull(IngestInfo.class, in.readOptionalWriteable(IngestInfo::new));
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_7_10_0)) {
            addInfoIfNonNull(AggregationInfo.class, in.readOptionalWriteable(AggregationInfo::new));
        }
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_8_0)) {
            addInfoIfNonNull(RemoteClusterServerInfo.class, in.readOptionalWriteable(RemoteClusterServerInfo::new));
        }
    }

    public NodeInfo(
        String version,
        TransportVersion transportVersion,
        IndexVersion indexVersion,
        Map<String, Integer> componentVersions,
        Build build,
        DiscoveryNode node,
        @Nullable Settings settings,
        @Nullable OsInfo os,
        @Nullable ProcessInfo process,
        @Nullable JvmInfo jvm,
        @Nullable ThreadPoolInfo threadPool,
        @Nullable TransportInfo transport,
        @Nullable HttpInfo http,
        @Nullable RemoteClusterServerInfo remoteClusterServer,
        @Nullable PluginsAndModules plugins,
        @Nullable IngestInfo ingest,
        @Nullable AggregationInfo aggsInfo,
        @Nullable ByteSizeValue totalIndexingBuffer
    ) {
        super(node);
        this.version = version;
        this.transportVersion = transportVersion;
        this.indexVersion = indexVersion;
        this.componentVersions = componentVersions;
        this.build = build;
        this.settings = settings;
        addInfoIfNonNull(OsInfo.class, os);
        addInfoIfNonNull(ProcessInfo.class, process);
        addInfoIfNonNull(JvmInfo.class, jvm);
        addInfoIfNonNull(ThreadPoolInfo.class, threadPool);
        addInfoIfNonNull(TransportInfo.class, transport);
        addInfoIfNonNull(HttpInfo.class, http);
        addInfoIfNonNull(RemoteClusterServerInfo.class, remoteClusterServer);
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
    public String getVersion() {
        return version;
    }

    /**
     * The most recent transport version that can be used by this node
     */
    public TransportVersion getTransportVersion() {
        return transportVersion;
    }

    /**
     * The most recent index version that can be used by this node
     */
    public IndexVersion getIndexVersion() {
        return indexVersion;
    }

    /**
     * The version numbers of other installed components
     */
    public Map<String, Integer> getComponentVersions() {
        return componentVersions;
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
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_12_0)) {
            out.writeString(version);
        } else {
            Version.writeVersion(Version.fromString(version), out);
        }
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_8_0)) {
            TransportVersion.writeVersion(transportVersion, out);
        }
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_11_X)) {
            IndexVersion.writeVersion(indexVersion, out);
            out.writeMap(componentVersions, StreamOutput::writeString, StreamOutput::writeVInt);
        }
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
            settings.writeTo(out);
        }
        out.writeOptionalWriteable(getInfo(OsInfo.class));
        out.writeOptionalWriteable(getInfo(ProcessInfo.class));
        out.writeOptionalWriteable(getInfo(JvmInfo.class));
        out.writeOptionalWriteable(getInfo(ThreadPoolInfo.class));
        out.writeOptionalWriteable(getInfo(TransportInfo.class));
        out.writeOptionalWriteable(getInfo(HttpInfo.class));
        out.writeOptionalWriteable(getInfo(PluginsAndModules.class));
        out.writeOptionalWriteable(getInfo(IngestInfo.class));
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_7_10_0)) {
            out.writeOptionalWriteable(getInfo(AggregationInfo.class));
        }
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_8_0)) {
            out.writeOptionalWriteable(getInfo(RemoteClusterServerInfo.class));
        }
    }
}
