/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.stats;

import com.carrotsearch.hppc.ObjectIntHashMap;
import com.carrotsearch.hppc.cursors.ObjectIntCursor;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.PluginsAndModules;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.discovery.DiscoveryModule;
import org.elasticsearch.index.stats.IndexingPressureStats;
import org.elasticsearch.monitor.fs.FsInfo;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.monitor.os.OsInfo;
import org.elasticsearch.plugins.PluginDescriptor;
import org.elasticsearch.transport.TransportInfo;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ClusterStatsNodes implements ToXContentFragment {

    private final Counts counts;
    private final Set<Version> versions;
    private final OsStats os;
    private final ProcessStats process;
    private final JvmStats jvm;
    private final FsInfo.Path fs;
    private final Set<PluginDescriptor> plugins;
    private final NetworkTypes networkTypes;
    private final DiscoveryTypes discoveryTypes;
    private final PackagingTypes packagingTypes;
    private final IngestStats ingestStats;
    private final IndexPressureStats indexPressureStats;

    ClusterStatsNodes(List<ClusterStatsNodeResponse> nodeResponses) {
        this.versions = new HashSet<>();
        this.fs = new FsInfo.Path();
        this.plugins = new HashSet<>();

        Set<InetAddress> seenAddresses = new HashSet<>(nodeResponses.size());
        List<NodeInfo> nodeInfos = new ArrayList<>(nodeResponses.size());
        List<NodeStats> nodeStats = new ArrayList<>(nodeResponses.size());
        for (ClusterStatsNodeResponse nodeResponse : nodeResponses) {
            nodeInfos.add(nodeResponse.nodeInfo());
            nodeStats.add(nodeResponse.nodeStats());
            this.versions.add(nodeResponse.nodeInfo().getVersion());
            this.plugins.addAll(nodeResponse.nodeInfo().getInfo(PluginsAndModules.class).getPluginInfos());

            // now do the stats that should be deduped by hardware (implemented by ip deduping)
            TransportAddress publishAddress = nodeResponse.nodeInfo().getInfo(TransportInfo.class).address().publishAddress();
            final InetAddress inetAddress = publishAddress.address().getAddress();
            if (seenAddresses.add(inetAddress) == false) {
                continue;
            }
            if (nodeResponse.nodeStats().getFs() != null) {
                this.fs.add(nodeResponse.nodeStats().getFs().getTotal());
            }
        }
        this.counts = new Counts(nodeInfos);
        this.os = new OsStats(nodeInfos, nodeStats);
        this.process = new ProcessStats(nodeStats);
        this.jvm = new JvmStats(nodeInfos, nodeStats);
        this.networkTypes = new NetworkTypes(nodeInfos);
        this.discoveryTypes = new DiscoveryTypes(nodeInfos);
        this.packagingTypes = new PackagingTypes(nodeInfos);
        this.ingestStats = new IngestStats(nodeStats);
        this.indexPressureStats = new IndexPressureStats(nodeStats);
    }

    public Counts getCounts() {
        return this.counts;
    }

    public Set<Version> getVersions() {
        return versions;
    }

    public OsStats getOs() {
        return os;
    }

    public ProcessStats getProcess() {
        return process;
    }

    public JvmStats getJvm() {
        return jvm;
    }

    public FsInfo.Path getFs() {
        return fs;
    }

    public Set<PluginDescriptor> getPlugins() {
        return plugins;
    }

    static final class Fields {
        static final String COUNT = "count";
        static final String VERSIONS = "versions";
        static final String OS = "os";
        static final String PROCESS = "process";
        static final String JVM = "jvm";
        static final String FS = "fs";
        static final String PLUGINS = "plugins";
        static final String NETWORK_TYPES = "network_types";
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.COUNT);
        counts.toXContent(builder, params);
        builder.endObject();

        builder.startArray(Fields.VERSIONS);
        for (Version v : versions) {
            builder.value(v.toString());
        }
        builder.endArray();

        builder.startObject(Fields.OS);
        os.toXContent(builder, params);
        builder.endObject();

        builder.startObject(Fields.PROCESS);
        process.toXContent(builder, params);
        builder.endObject();

        builder.startObject(Fields.JVM);
        jvm.toXContent(builder, params);
        builder.endObject();

        builder.field(Fields.FS);
        fs.toXContent(builder, params);

        builder.startArray(Fields.PLUGINS);
        for (PluginDescriptor pluginDescriptor : plugins) {
            pluginDescriptor.toXContent(builder, params);
        }
        builder.endArray();

        builder.startObject(Fields.NETWORK_TYPES);
        networkTypes.toXContent(builder, params);
        builder.endObject();

        discoveryTypes.toXContent(builder, params);

        packagingTypes.toXContent(builder, params);

        ingestStats.toXContent(builder, params);

        indexPressureStats.toXContent(builder, params);

        return builder;
    }

    public static class Counts implements ToXContentFragment {
        static final String COORDINATING_ONLY = "coordinating_only";

        private final int total;
        private final Map<String, Integer> roles;

        private Counts(final List<NodeInfo> nodeInfos) {
            // TODO: do we need to report zeros?
            final Map<String, Integer> roles = Maps.newMapWithExpectedSize(DiscoveryNodeRole.roles().size() + 1);
            roles.put(COORDINATING_ONLY, 0);
            for (final DiscoveryNodeRole role : DiscoveryNodeRole.roles()) {
                roles.put(role.roleName(), 0);
            }

            int total = 0;
            for (final NodeInfo nodeInfo : nodeInfos) {
                total++;
                if (nodeInfo.getNode().getRoles().isEmpty()) {
                    roles.merge(COORDINATING_ONLY, 1, Integer::sum);
                } else {
                    for (DiscoveryNodeRole role : nodeInfo.getNode().getRoles()) {
                        roles.merge(role.roleName(), 1, Integer::sum);
                    }
                }
            }
            this.total = total;
            this.roles = Map.copyOf(roles);
        }

        public int getTotal() {
            return total;
        }

        public Map<String, Integer> getRoles() {
            return roles;
        }

        static final class Fields {
            static final String TOTAL = "total";
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(Fields.TOTAL, total);
            for (Map.Entry<String, Integer> entry : new TreeMap<>(roles).entrySet()) {
                builder.field(entry.getKey(), entry.getValue());
            }
            return builder;
        }
    }

    public static class OsStats implements ToXContentFragment {
        final int availableProcessors;
        final int allocatedProcessors;
        final ObjectIntHashMap<String> names;
        final ObjectIntHashMap<String> prettyNames;
        final ObjectIntHashMap<String> architectures;
        final org.elasticsearch.monitor.os.OsStats.Mem mem;

        /**
         * Build the stats from information about each node.
         */
        private OsStats(List<NodeInfo> nodeInfos, List<NodeStats> nodeStatsList) {
            this.names = new ObjectIntHashMap<>();
            this.prettyNames = new ObjectIntHashMap<>();
            this.architectures = new ObjectIntHashMap<>();
            int availableProcessors = 0;
            int allocatedProcessors = 0;
            for (NodeInfo nodeInfo : nodeInfos) {
                availableProcessors += nodeInfo.getInfo(OsInfo.class).getAvailableProcessors();
                allocatedProcessors += nodeInfo.getInfo(OsInfo.class).getAllocatedProcessors();

                if (nodeInfo.getInfo(OsInfo.class).getName() != null) {
                    names.addTo(nodeInfo.getInfo(OsInfo.class).getName(), 1);
                }
                if (nodeInfo.getInfo(OsInfo.class).getPrettyName() != null) {
                    prettyNames.addTo(nodeInfo.getInfo(OsInfo.class).getPrettyName(), 1);
                }
                if (nodeInfo.getInfo(OsInfo.class).getArch() != null) {
                    architectures.addTo(nodeInfo.getInfo(OsInfo.class).getArch(), 1);
                }
            }
            this.availableProcessors = availableProcessors;
            this.allocatedProcessors = allocatedProcessors;

            long totalMemory = 0;
            long adjustedTotalMemory = 0;
            long freeMemory = 0;
            for (NodeStats nodeStats : nodeStatsList) {
                if (nodeStats.getOs() != null) {
                    org.elasticsearch.monitor.os.OsStats.Mem mem = nodeStats.getOs().getMem();
                    long total = mem.getTotal().getBytes();
                    if (total > 0) {
                        totalMemory += total;
                    }
                    long adjustedTotal = mem.getAdjustedTotal().getBytes();
                    if (adjustedTotal > 0) {
                        adjustedTotalMemory += adjustedTotal;
                    }
                    long free = mem.getFree().getBytes();
                    if (free > 0) {
                        freeMemory += free;
                    }
                }
            }
            this.mem = new org.elasticsearch.monitor.os.OsStats.Mem(totalMemory, adjustedTotalMemory, freeMemory);
        }

        public int getAvailableProcessors() {
            return availableProcessors;
        }

        public int getAllocatedProcessors() {
            return allocatedProcessors;
        }

        public org.elasticsearch.monitor.os.OsStats.Mem getMem() {
            return mem;
        }

        static final class Fields {
            static final String AVAILABLE_PROCESSORS = "available_processors";
            static final String ALLOCATED_PROCESSORS = "allocated_processors";
            static final String NAME = "name";
            static final String NAMES = "names";
            static final String PRETTY_NAME = "pretty_name";
            static final String PRETTY_NAMES = "pretty_names";
            static final String ARCH = "arch";
            static final String ARCHITECTURES = "architectures";
            static final String COUNT = "count";
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(Fields.AVAILABLE_PROCESSORS, availableProcessors);
            builder.field(Fields.ALLOCATED_PROCESSORS, allocatedProcessors);
            builder.startArray(Fields.NAMES);
            {
                for (ObjectIntCursor<String> name : names) {
                    builder.startObject();
                    {
                        builder.field(Fields.NAME, name.key);
                        builder.field(Fields.COUNT, name.value);
                    }
                    builder.endObject();
                }
            }
            builder.endArray();
            builder.startArray(Fields.PRETTY_NAMES);
            {
                for (final ObjectIntCursor<String> prettyName : prettyNames) {
                    builder.startObject();
                    {
                        builder.field(Fields.PRETTY_NAME, prettyName.key);
                        builder.field(Fields.COUNT, prettyName.value);
                    }
                    builder.endObject();
                }
            }
            builder.endArray();
            builder.startArray(Fields.ARCHITECTURES);
            {
                for (final ObjectIntCursor<String> arch : architectures) {
                    builder.startObject();
                    {
                        builder.field(Fields.ARCH, arch.key);
                        builder.field(Fields.COUNT, arch.value);
                    }
                    builder.endObject();
                }
            }
            builder.endArray();
            mem.toXContent(builder, params);
            return builder;
        }
    }

    public static class ProcessStats implements ToXContentFragment {

        final int count;
        final int cpuPercent;
        final long totalOpenFileDescriptors;
        final long minOpenFileDescriptors;
        final long maxOpenFileDescriptors;

        /**
         * Build from looking at a list of node statistics.
         */
        private ProcessStats(List<NodeStats> nodeStatsList) {
            int count = 0;
            int cpuPercent = 0;
            long totalOpenFileDescriptors = 0;
            long minOpenFileDescriptors = Long.MAX_VALUE;
            long maxOpenFileDescriptors = Long.MIN_VALUE;
            for (NodeStats nodeStats : nodeStatsList) {
                if (nodeStats.getProcess() == null) {
                    continue;
                }
                count++;
                if (nodeStats.getProcess().getCpu() != null) {
                    cpuPercent += nodeStats.getProcess().getCpu().getPercent();
                }
                long fd = nodeStats.getProcess().getOpenFileDescriptors();
                if (fd > 0) {
                    // fd can be -1 if not supported on platform
                    totalOpenFileDescriptors += fd;
                }
                // we still do min max calc on -1, so we'll have an indication
                // of it not being supported on one of the nodes.
                minOpenFileDescriptors = Math.min(minOpenFileDescriptors, fd);
                maxOpenFileDescriptors = Math.max(maxOpenFileDescriptors, fd);
            }
            this.count = count;
            this.cpuPercent = cpuPercent;
            this.totalOpenFileDescriptors = totalOpenFileDescriptors;
            this.minOpenFileDescriptors = minOpenFileDescriptors;
            this.maxOpenFileDescriptors = maxOpenFileDescriptors;
        }

        /**
         * Cpu usage in percentages - 100 is 1 core.
         */
        public int getCpuPercent() {
            return cpuPercent;
        }

        public long getAvgOpenFileDescriptors() {
            if (count == 0) {
                return -1;
            }
            return totalOpenFileDescriptors / count;
        }

        public long getMaxOpenFileDescriptors() {
            if (count == 0) {
                return -1;
            }
            return maxOpenFileDescriptors;
        }

        public long getMinOpenFileDescriptors() {
            if (count == 0) {
                return -1;
            }
            return minOpenFileDescriptors;
        }

        static final class Fields {
            static final String CPU = "cpu";
            static final String PERCENT = "percent";
            static final String OPEN_FILE_DESCRIPTORS = "open_file_descriptors";
            static final String MIN = "min";
            static final String MAX = "max";
            static final String AVG = "avg";
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject(Fields.CPU).field(Fields.PERCENT, cpuPercent).endObject();
            if (count > 0) {
                builder.startObject(Fields.OPEN_FILE_DESCRIPTORS);
                builder.field(Fields.MIN, getMinOpenFileDescriptors());
                builder.field(Fields.MAX, getMaxOpenFileDescriptors());
                builder.field(Fields.AVG, getAvgOpenFileDescriptors());
                builder.endObject();
            }
            return builder;
        }
    }

    public static class JvmStats implements ToXContentFragment {

        private final ObjectIntHashMap<JvmVersion> versions;
        private final long threads;
        private final long maxUptime;
        private final long heapUsed;
        private final long heapMax;

        /**
         * Build from lists of information about each node.
         */
        private JvmStats(List<NodeInfo> nodeInfos, List<NodeStats> nodeStatsList) {
            this.versions = new ObjectIntHashMap<>();
            long threads = 0;
            long maxUptime = 0;
            long heapMax = 0;
            long heapUsed = 0;
            for (NodeInfo nodeInfo : nodeInfos) {
                versions.addTo(new JvmVersion(nodeInfo.getInfo(JvmInfo.class)), 1);
            }

            for (NodeStats nodeStats : nodeStatsList) {
                org.elasticsearch.monitor.jvm.JvmStats js = nodeStats.getJvm();
                if (js == null) {
                    continue;
                }
                if (js.getThreads() != null) {
                    threads += js.getThreads().getCount();
                }
                maxUptime = Math.max(maxUptime, js.getUptime().millis());
                if (js.getMem() != null) {
                    heapUsed += js.getMem().getHeapUsed().getBytes();
                    heapMax += js.getMem().getHeapMax().getBytes();
                }
            }
            this.threads = threads;
            this.maxUptime = maxUptime;
            this.heapUsed = heapUsed;
            this.heapMax = heapMax;
        }

        public ObjectIntHashMap<JvmVersion> getVersions() {
            return versions;
        }

        /**
         * The total number of threads in the cluster
         */
        public long getThreads() {
            return threads;
        }

        /**
         * The maximum uptime of a node in the cluster
         */
        public TimeValue getMaxUpTime() {
            return new TimeValue(maxUptime);
        }

        /**
         * Total heap used in the cluster
         */
        public ByteSizeValue getHeapUsed() {
            return new ByteSizeValue(heapUsed);
        }

        /**
         * Maximum total heap available to the cluster
         */
        public ByteSizeValue getHeapMax() {
            return new ByteSizeValue(heapMax);
        }

        static final class Fields {
            static final String VERSIONS = "versions";
            static final String VERSION = "version";
            static final String VM_NAME = "vm_name";
            static final String VM_VERSION = "vm_version";
            static final String VM_VENDOR = "vm_vendor";
            static final String BUNDLED_JDK = "bundled_jdk";
            static final String USING_BUNDLED_JDK = "using_bundled_jdk";
            static final String COUNT = "count";
            static final String THREADS = "threads";
            static final String MAX_UPTIME = "max_uptime";
            static final String MAX_UPTIME_IN_MILLIS = "max_uptime_in_millis";
            static final String MEM = "mem";
            static final String HEAP_USED = "heap_used";
            static final String HEAP_USED_IN_BYTES = "heap_used_in_bytes";
            static final String HEAP_MAX = "heap_max";
            static final String HEAP_MAX_IN_BYTES = "heap_max_in_bytes";
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.humanReadableField(Fields.MAX_UPTIME_IN_MILLIS, Fields.MAX_UPTIME, new TimeValue(maxUptime));
            builder.startArray(Fields.VERSIONS);
            for (ObjectIntCursor<JvmVersion> v : versions) {
                builder.startObject();
                builder.field(Fields.VERSION, v.key.version);
                builder.field(Fields.VM_NAME, v.key.vmName);
                builder.field(Fields.VM_VERSION, v.key.vmVersion);
                builder.field(Fields.VM_VENDOR, v.key.vmVendor);
                builder.field(Fields.BUNDLED_JDK, v.key.bundledJdk);
                builder.field(Fields.USING_BUNDLED_JDK, v.key.usingBundledJdk);
                builder.field(Fields.COUNT, v.value);
                builder.endObject();
            }
            builder.endArray();
            builder.startObject(Fields.MEM);
            builder.humanReadableField(Fields.HEAP_USED_IN_BYTES, Fields.HEAP_USED, getHeapUsed());
            builder.humanReadableField(Fields.HEAP_MAX_IN_BYTES, Fields.HEAP_MAX, getHeapMax());
            builder.endObject();

            builder.field(Fields.THREADS, threads);
            return builder;
        }
    }

    public static class JvmVersion {
        String version;
        String vmName;
        String vmVersion;
        String vmVendor;
        boolean bundledJdk;
        Boolean usingBundledJdk;

        JvmVersion(JvmInfo jvmInfo) {
            version = jvmInfo.version();
            vmName = jvmInfo.getVmName();
            vmVersion = jvmInfo.getVmVersion();
            vmVendor = jvmInfo.getVmVendor();
            bundledJdk = jvmInfo.getBundledJdk();
            usingBundledJdk = jvmInfo.getUsingBundledJdk();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            JvmVersion jvm = (JvmVersion) o;

            return vmVersion.equals(jvm.vmVersion) && vmVendor.equals(jvm.vmVendor);
        }

        @Override
        public int hashCode() {
            return vmVersion.hashCode();
        }
    }

    static class NetworkTypes implements ToXContentFragment {

        private final Map<String, AtomicInteger> transportTypes;
        private final Map<String, AtomicInteger> httpTypes;

        NetworkTypes(final List<NodeInfo> nodeInfos) {
            final Map<String, AtomicInteger> transportTypes = new HashMap<>();
            final Map<String, AtomicInteger> httpTypes = new HashMap<>();
            for (final NodeInfo nodeInfo : nodeInfos) {
                final Settings settings = nodeInfo.getSettings();
                final String transportType = settings.get(
                    NetworkModule.TRANSPORT_TYPE_KEY,
                    NetworkModule.TRANSPORT_DEFAULT_TYPE_SETTING.get(settings)
                );
                final String httpType = settings.get(NetworkModule.HTTP_TYPE_KEY, NetworkModule.HTTP_DEFAULT_TYPE_SETTING.get(settings));
                if (Strings.hasText(transportType)) {
                    transportTypes.computeIfAbsent(transportType, k -> new AtomicInteger()).incrementAndGet();
                }
                if (Strings.hasText(httpType)) {
                    httpTypes.computeIfAbsent(httpType, k -> new AtomicInteger()).incrementAndGet();
                }
            }
            this.transportTypes = Collections.unmodifiableMap(transportTypes);
            this.httpTypes = Collections.unmodifiableMap(httpTypes);
        }

        @Override
        public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
            builder.startObject("transport_types");
            for (final Map.Entry<String, AtomicInteger> entry : transportTypes.entrySet()) {
                builder.field(entry.getKey(), entry.getValue().get());
            }
            builder.endObject();
            builder.startObject("http_types");
            for (final Map.Entry<String, AtomicInteger> entry : httpTypes.entrySet()) {
                builder.field(entry.getKey(), entry.getValue().get());
            }
            builder.endObject();
            return builder;
        }

    }

    static class DiscoveryTypes implements ToXContentFragment {

        private final Map<String, AtomicInteger> discoveryTypes;

        DiscoveryTypes(final List<NodeInfo> nodeInfos) {
            final Map<String, AtomicInteger> discoveryTypes = new HashMap<>();
            for (final NodeInfo nodeInfo : nodeInfos) {
                final Settings settings = nodeInfo.getSettings();
                final String discoveryType = DiscoveryModule.DISCOVERY_TYPE_SETTING.get(settings);
                discoveryTypes.computeIfAbsent(discoveryType, k -> new AtomicInteger()).incrementAndGet();
            }
            this.discoveryTypes = Collections.unmodifiableMap(discoveryTypes);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject("discovery_types");
            for (final Map.Entry<String, AtomicInteger> entry : discoveryTypes.entrySet()) {
                builder.field(entry.getKey(), entry.getValue().get());
            }
            builder.endObject();
            return builder;
        }
    }

    static class PackagingTypes implements ToXContentFragment {

        private final Map<Tuple<String, String>, AtomicInteger> packagingTypes;

        PackagingTypes(final List<NodeInfo> nodeInfos) {
            final var packagingTypes = new HashMap<Tuple<String, String>, AtomicInteger>();
            for (final var nodeInfo : nodeInfos) {
                final var flavor = nodeInfo.getBuild().flavor().displayName();
                final var type = nodeInfo.getBuild().type().displayName();
                packagingTypes.computeIfAbsent(Tuple.tuple(flavor, type), k -> new AtomicInteger()).incrementAndGet();
            }
            this.packagingTypes = Collections.unmodifiableMap(packagingTypes);
        }

        @Override
        public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
            builder.startArray("packaging_types");
            {
                for (final var entry : packagingTypes.entrySet()) {
                    builder.startObject();
                    {
                        builder.field("flavor", entry.getKey().v1());
                        builder.field("type", entry.getKey().v2());
                        builder.field("count", entry.getValue().get());
                    }
                    builder.endObject();
                }
            }
            builder.endArray();
            return builder;
        }

    }

    static class IngestStats implements ToXContentFragment {

        final int pipelineCount;
        final SortedMap<String, long[]> stats;

        IngestStats(final List<NodeStats> nodeStats) {
            Set<String> pipelineIds = new HashSet<>();
            SortedMap<String, long[]> stats = new TreeMap<>();
            for (NodeStats nodeStat : nodeStats) {
                if (nodeStat.getIngestStats() != null) {
                    for (Map.Entry<String, List<org.elasticsearch.ingest.IngestStats.ProcessorStat>> processorStats : nodeStat
                        .getIngestStats()
                        .getProcessorStats()
                        .entrySet()) {
                        pipelineIds.add(processorStats.getKey());
                        for (org.elasticsearch.ingest.IngestStats.ProcessorStat stat : processorStats.getValue()) {
                            stats.compute(stat.getType(), (k, v) -> {
                                org.elasticsearch.ingest.IngestStats.Stats nodeIngestStats = stat.getStats();
                                if (v == null) {
                                    return new long[] {
                                        nodeIngestStats.getIngestCount(),
                                        nodeIngestStats.getIngestFailedCount(),
                                        nodeIngestStats.getIngestCurrent(),
                                        nodeIngestStats.getIngestTimeInMillis() };
                                } else {
                                    v[0] += nodeIngestStats.getIngestCount();
                                    v[1] += nodeIngestStats.getIngestFailedCount();
                                    v[2] += nodeIngestStats.getIngestCurrent();
                                    v[3] += nodeIngestStats.getIngestTimeInMillis();
                                    return v;
                                }
                            });
                        }
                    }
                }
            }
            this.pipelineCount = pipelineIds.size();
            this.stats = Collections.unmodifiableSortedMap(stats);
        }

        @Override
        public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
            builder.startObject("ingest");
            {
                builder.field("number_of_pipelines", pipelineCount);
                builder.startObject("processor_stats");
                for (Map.Entry<String, long[]> stat : stats.entrySet()) {
                    long[] statValues = stat.getValue();
                    builder.startObject(stat.getKey());
                    builder.field("count", statValues[0]);
                    builder.field("failed", statValues[1]);
                    builder.field("current", statValues[2]);
                    builder.humanReadableField("time_in_millis", "time", new TimeValue(statValues[3], TimeUnit.MILLISECONDS));
                    builder.endObject();
                }
                builder.endObject();
            }
            builder.endObject();
            return builder;
        }

    }

    static class IndexPressureStats implements ToXContentFragment {

        private final IndexingPressureStats indexingPressureStats;

        IndexPressureStats(final List<NodeStats> nodeStats) {
            long totalCombinedCoordinatingAndPrimaryBytes = 0;
            long totalCoordinatingBytes = 0;
            long totalPrimaryBytes = 0;
            long totalReplicaBytes = 0;

            long currentCombinedCoordinatingAndPrimaryBytes = 0;
            long currentCoordinatingBytes = 0;
            long currentPrimaryBytes = 0;
            long currentReplicaBytes = 0;
            long coordinatingRejections = 0;
            long primaryRejections = 0;
            long replicaRejections = 0;
            long memoryLimit = 0;

            long totalCoordinatingOps = 0;
            long totalPrimaryOps = 0;
            long totalReplicaOps = 0;
            long currentCoordinatingOps = 0;
            long currentPrimaryOps = 0;
            long currentReplicaOps = 0;
            for (NodeStats nodeStat : nodeStats) {
                IndexingPressureStats nodeStatIndexingPressureStats = nodeStat.getIndexingPressureStats();
                if (nodeStatIndexingPressureStats != null) {
                    totalCombinedCoordinatingAndPrimaryBytes += nodeStatIndexingPressureStats.getTotalCombinedCoordinatingAndPrimaryBytes();
                    totalCoordinatingBytes += nodeStatIndexingPressureStats.getTotalCoordinatingBytes();
                    totalPrimaryBytes += nodeStatIndexingPressureStats.getTotalPrimaryBytes();
                    totalReplicaBytes += nodeStatIndexingPressureStats.getTotalReplicaBytes();
                    currentCombinedCoordinatingAndPrimaryBytes += nodeStatIndexingPressureStats
                        .getCurrentCombinedCoordinatingAndPrimaryBytes();
                    currentCoordinatingBytes += nodeStatIndexingPressureStats.getCurrentCoordinatingBytes();
                    currentPrimaryBytes += nodeStatIndexingPressureStats.getCurrentPrimaryBytes();
                    currentReplicaBytes += nodeStatIndexingPressureStats.getCurrentReplicaBytes();
                    coordinatingRejections += nodeStatIndexingPressureStats.getCoordinatingRejections();
                    primaryRejections += nodeStatIndexingPressureStats.getPrimaryRejections();
                    replicaRejections += nodeStatIndexingPressureStats.getReplicaRejections();
                    memoryLimit += nodeStatIndexingPressureStats.getMemoryLimit();
                    totalCoordinatingOps += nodeStatIndexingPressureStats.getTotalCoordinatingOps();
                    totalReplicaOps += nodeStatIndexingPressureStats.getTotalReplicaOps();
                    currentCoordinatingOps += nodeStatIndexingPressureStats.getCurrentCoordinatingOps();
                    currentPrimaryOps += nodeStatIndexingPressureStats.getCurrentPrimaryOps();
                    currentReplicaOps += nodeStatIndexingPressureStats.getCurrentReplicaOps();
                }
            }
            indexingPressureStats = new IndexingPressureStats(
                totalCombinedCoordinatingAndPrimaryBytes,
                totalCoordinatingBytes,
                totalPrimaryBytes,
                totalReplicaBytes,
                currentCombinedCoordinatingAndPrimaryBytes,
                currentCoordinatingBytes,
                currentPrimaryBytes,
                currentReplicaBytes,
                coordinatingRejections,
                primaryRejections,
                replicaRejections,
                memoryLimit,
                totalCoordinatingOps,
                totalPrimaryOps,
                totalReplicaOps,
                currentCoordinatingOps,
                currentPrimaryOps,
                currentReplicaOps
            );
        }

        @Override
        public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
            return indexingPressureStats.toXContent(builder, params);
        }

    }

}
