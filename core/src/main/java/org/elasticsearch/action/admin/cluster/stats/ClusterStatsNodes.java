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

package org.elasticsearch.action.admin.cluster.stats;

import com.carrotsearch.hppc.ObjectIntHashMap;
import com.carrotsearch.hppc.cursors.ObjectIntCursor;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.monitor.fs.FsInfo;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.plugins.PluginInfo;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ClusterStatsNodes implements ToXContent, Writeable<ClusterStatsNodes> {

    private final Counts counts;
    private final Set<Version> versions;
    private final OsStats os;
    private final ProcessStats process;
    private final JvmStats jvm;
    private final FsInfo.Path fs;
    private final Set<PluginInfo> plugins;

    ClusterStatsNodes(StreamInput in) throws IOException {
        this.counts = new Counts(in);

        int size = in.readVInt();
        this.versions = new HashSet<>(size);
        for (int i = 0; i < size; i++) {
            this.versions.add(Version.readVersion(in));
        }

        this.os = new OsStats(in);
        this.process = new ProcessStats(in);
        this.jvm = new JvmStats(in);
        this.fs = FsInfo.Path.readInfoFrom(in);

        size = in.readVInt();
        this.plugins = new HashSet<>(size);
        for (int i = 0; i < size; i++) {
            this.plugins.add(PluginInfo.readFromStream(in));
        }
    }

    ClusterStatsNodes(ClusterStatsNodeResponse[] nodeResponses) {
        this.versions = new HashSet<>();
        this.fs = new FsInfo.Path();
        this.plugins = new HashSet<>();

        Set<InetAddress> seenAddresses = new HashSet<>(nodeResponses.length);
        List<NodeInfo> nodeInfos = new ArrayList<>();
        List<NodeStats> nodeStats = new ArrayList<>();
        for (ClusterStatsNodeResponse nodeResponse : nodeResponses) {
            nodeInfos.add(nodeResponse.nodeInfo());
            nodeStats.add(nodeResponse.nodeStats());
            this.versions.add(nodeResponse.nodeInfo().getVersion());
            this.plugins.addAll(nodeResponse.nodeInfo().getPlugins().getPluginInfos());

            // now do the stats that should be deduped by hardware (implemented by ip deduping)
            TransportAddress publishAddress = nodeResponse.nodeInfo().getTransport().address().publishAddress();
            InetAddress inetAddress = null;
            if (publishAddress.uniqueAddressTypeId() == 1) {
                inetAddress = ((InetSocketTransportAddress) publishAddress).address().getAddress();
            }
            if (!seenAddresses.add(inetAddress)) {
                continue;
            }
            if (nodeResponse.nodeStats().getFs() != null) {
                this.fs.add(nodeResponse.nodeStats().getFs().total());
            }
        }
        this.counts = new Counts(nodeInfos);
        this.os = new OsStats(nodeInfos);
        this.process = new ProcessStats(nodeStats);
        this.jvm = new JvmStats(nodeInfos, nodeStats);
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

    public Set<PluginInfo> getPlugins() {
        return plugins;
    }


    @Override
    public ClusterStatsNodes readFrom(StreamInput in) throws IOException {
        return new ClusterStatsNodes(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        counts.writeTo(out);
        out.writeVInt(versions.size());
        for (Version v : versions) Version.writeVersion(v, out);
        os.writeTo(out);
        process.writeTo(out);
        jvm.writeTo(out);
        fs.writeTo(out);
        out.writeVInt(plugins.size());
        for (PluginInfo p : plugins) {
            p.writeTo(out);
        }
    }

    static final class Fields {
        static final String COUNT = "count";
        static final String VERSIONS = "versions";
        static final String OS = "os";
        static final String PROCESS = "process";
        static final String JVM = "jvm";
        static final String FS = "fs";
        static final String PLUGINS = "plugins";
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
        for (PluginInfo pluginInfo : plugins) {
            pluginInfo.toXContent(builder, params);
        }
        builder.endArray();
        return builder;
    }

    public static class Counts implements Writeable<Counts>, ToXContent {
        static final String COORDINATING_ONLY = "coordinating_only";

        private final int total;
        private final Map<String, Integer> roles;

        @SuppressWarnings("unchecked")
        private Counts(StreamInput in) throws IOException {
            this.total = in.readVInt();
            this.roles = (Map<String, Integer>)in.readGenericValue();
        }

        private Counts(List<NodeInfo> nodeInfos) {
            this.roles = new HashMap<>();
            for (DiscoveryNode.Role role : DiscoveryNode.Role.values()) {
                this.roles.put(role.getRoleName(), 0);
            }
            this.roles.put(COORDINATING_ONLY, 0);

            int total = 0;
            for (NodeInfo nodeInfo : nodeInfos) {
                total++;
                if (nodeInfo.getNode().getRoles().isEmpty()) {
                    Integer count = roles.get(COORDINATING_ONLY);
                    roles.put(COORDINATING_ONLY, ++count);
                } else {
                    for (DiscoveryNode.Role role : nodeInfo.getNode().getRoles()) {
                        Integer count = roles.get(role.getRoleName());
                        roles.put(role.getRoleName(), ++count);
                    }
                }
            }
            this.total = total;
        }

        public int getTotal() {
            return total;
        }

        public Map<String, Integer> getRoles() {
            return roles;
        }

        @Override
        public Counts readFrom(StreamInput in) throws IOException {
            return new Counts(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(total);
            out.writeGenericValue(roles);
        }

        static final class Fields {
            static final String TOTAL = "total";
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(Fields.TOTAL, total);
            for (Map.Entry<String, Integer> entry : roles.entrySet()) {
                builder.field(entry.getKey(), entry.getValue());
            }
            return builder;
        }
    }

    public static class OsStats implements ToXContent, Writeable<OsStats> {
        final int availableProcessors;
        final int allocatedProcessors;
        final ObjectIntHashMap<String> names;

        @SuppressWarnings("unchecked")
        private OsStats(StreamInput in) throws IOException {
            this.availableProcessors = in.readVInt();
            this.allocatedProcessors = in.readVInt();
            int size = in.readVInt();
            this.names = new ObjectIntHashMap<>();
            for (int i = 0; i < size; i++) {
                names.addTo(in.readString(), in.readVInt());
            }
        }

        private OsStats(List<NodeInfo> nodeInfos) {
            this.names = new ObjectIntHashMap<>();
            int availableProcessors = 0;
            int allocatedProcessors = 0;
            for (NodeInfo nodeInfo : nodeInfos) {
                availableProcessors += nodeInfo.getOs().getAvailableProcessors();
                allocatedProcessors += nodeInfo.getOs().getAllocatedProcessors();

                if (nodeInfo.getOs().getName() != null) {
                    names.addTo(nodeInfo.getOs().getName(), 1);
                }
            }
            this.availableProcessors = availableProcessors;
            this.allocatedProcessors = allocatedProcessors;
        }

        public int getAvailableProcessors() {
            return availableProcessors;
        }

        public int getAllocatedProcessors() {
            return allocatedProcessors;
        }

        @Override
        public OsStats readFrom(StreamInput in) throws IOException {
            return new OsStats(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(availableProcessors);
            out.writeVInt(allocatedProcessors);
            out.writeVInt(names.size());
            for (ObjectIntCursor<String> name : names) {
                out.writeString(name.key);
                out.writeVInt(name.value);
            }
        }

        static final class Fields {
            static final String AVAILABLE_PROCESSORS = "available_processors";
            static final String ALLOCATED_PROCESSORS = "allocated_processors";
            static final String NAME = "name";
            static final String NAMES = "names";
            static final String COUNT = "count";
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(Fields.AVAILABLE_PROCESSORS, availableProcessors);
            builder.field(Fields.ALLOCATED_PROCESSORS, allocatedProcessors);
            builder.startArray(Fields.NAMES);
            for (ObjectIntCursor<String> name : names) {
                builder.startObject();
                builder.field(Fields.NAME, name.key);
                builder.field(Fields.COUNT, name.value);
                builder.endObject();
            }
            builder.endArray();
            return builder;
        }
    }

    public static class ProcessStats implements ToXContent, Writeable<ProcessStats> {

        final int count;
        final int cpuPercent;
        final long totalOpenFileDescriptors;
        final long minOpenFileDescriptors;
        final long maxOpenFileDescriptors;

        private ProcessStats(StreamInput in) throws IOException {
            this.count = in.readVInt();
            this.cpuPercent = in.readVInt();
            this.totalOpenFileDescriptors = in.readVLong();
            this.minOpenFileDescriptors = in.readLong();
            this.maxOpenFileDescriptors = in.readLong();
        }

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
                // we still do min max calc on -1, so we'll have an indication of it not being supported on one of the nodes.
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

        @Override
        public ProcessStats readFrom(StreamInput in) throws IOException {
            return new ProcessStats(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(count);
            out.writeVInt(cpuPercent);
            out.writeVLong(totalOpenFileDescriptors);
            out.writeLong(minOpenFileDescriptors);
            out.writeLong(maxOpenFileDescriptors);
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

    public static class JvmStats implements Writeable<JvmStats>, ToXContent {

        private final ObjectIntHashMap<JvmVersion> versions;
        private final long threads;
        private final long maxUptime;
        private final long heapUsed;
        private final long heapMax;

        private JvmStats(StreamInput in) throws IOException {
            int size = in.readVInt();
            this.versions = new ObjectIntHashMap<>(size);
            for (int i = 0; i < size; i++) {
                this.versions.addTo(JvmVersion.readJvmVersion(in), in.readVInt());
            }
            this.threads = in.readVLong();
            this.maxUptime = in.readVLong();
            this.heapUsed = in.readVLong();
            this.heapMax = in.readVLong();
        }

        private JvmStats(List<NodeInfo> nodeInfos, List<NodeStats> nodeStatsList) {
            this.versions = new ObjectIntHashMap<>();
            long threads = 0;
            long maxUptime = 0;
            long heapMax = 0;
            long heapUsed = 0;
            for (NodeInfo nodeInfo : nodeInfos) {
                versions.addTo(new JvmVersion(nodeInfo.getJvm()), 1);
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
                    heapUsed += js.getMem().getHeapUsed().bytes();
                    heapMax += js.getMem().getHeapMax().bytes();
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

        @Override
        public JvmStats readFrom(StreamInput in) throws IOException {
            return new JvmStats(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(versions.size());
            for (ObjectIntCursor<JvmVersion> v : versions) {
                v.key.writeTo(out);
                out.writeVInt(v.value);
            }
            out.writeVLong(threads);
            out.writeVLong(maxUptime);
            out.writeVLong(heapUsed);
            out.writeVLong(heapMax);
        }

        static final class Fields {
            static final String VERSIONS = "versions";
            static final String VERSION = "version";
            static final String VM_NAME = "vm_name";
            static final String VM_VERSION = "vm_version";
            static final String VM_VENDOR = "vm_vendor";
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
            builder.timeValueField(Fields.MAX_UPTIME_IN_MILLIS, Fields.MAX_UPTIME, maxUptime);
            builder.startArray(Fields.VERSIONS);
            for (ObjectIntCursor<JvmVersion> v : versions) {
                builder.startObject();
                builder.field(Fields.VERSION, v.key.version);
                builder.field(Fields.VM_NAME, v.key.vmName);
                builder.field(Fields.VM_VERSION, v.key.vmVersion);
                builder.field(Fields.VM_VENDOR, v.key.vmVendor);
                builder.field(Fields.COUNT, v.value);
                builder.endObject();
            }
            builder.endArray();
            builder.startObject(Fields.MEM);
            builder.byteSizeField(Fields.HEAP_USED_IN_BYTES, Fields.HEAP_USED, heapUsed);
            builder.byteSizeField(Fields.HEAP_MAX_IN_BYTES, Fields.HEAP_MAX, heapMax);
            builder.endObject();

            builder.field(Fields.THREADS, threads);
            return builder;
        }
    }

    public static class JvmVersion implements Streamable {
        String version;
        String vmName;
        String vmVersion;
        String vmVendor;

        JvmVersion(JvmInfo jvmInfo) {
            version = jvmInfo.version();
            vmName = jvmInfo.getVmName();
            vmVersion = jvmInfo.getVmVersion();
            vmVendor = jvmInfo.getVmVendor();
        }

        JvmVersion() {
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

        public static JvmVersion readJvmVersion(StreamInput in) throws IOException {
            JvmVersion jvm = new JvmVersion();
            jvm.readFrom(in);
            return jvm;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            version = in.readString();
            vmName = in.readString();
            vmVersion = in.readString();
            vmVendor = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(version);
            out.writeString(vmName);
            out.writeString(vmVersion);
            out.writeString(vmVendor);
        }
    }
}
