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

package org.elasticsearch.monitor.fs;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.apache.lucene.util.Constants;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.DiskUsage;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.NodeEnvironment.NodePath;

import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class FsProbe extends AbstractComponent {

    private final NodeEnvironment nodeEnv;

    public FsProbe(Settings settings, NodeEnvironment nodeEnv) {
        super(settings);
        this.nodeEnv = nodeEnv;
    }

    public FsInfo stats(FsInfo previous, @Nullable ClusterInfo clusterInfo) throws IOException {
        if (!nodeEnv.hasNodeFile()) {
            return new FsInfo(System.currentTimeMillis(), null, new FsInfo.Path[0]);
        }
        NodePath[] dataLocations = nodeEnv.nodePaths();
        FsInfo.Path[] paths = new FsInfo.Path[dataLocations.length];
        for (int i = 0; i < dataLocations.length; i++) {
            paths[i] = getFSInfo(dataLocations[i]);
        }
        FsInfo.IoStats ioStats = null;
        if (Constants.LINUX) {
            Set<Tuple<Integer, Integer>> devicesNumbers = new HashSet<>();
            for (int i = 0; i < dataLocations.length; i++) {
                if (dataLocations[i].majorDeviceNumber != -1 && dataLocations[i].minorDeviceNumber != -1) {
                    devicesNumbers.add(Tuple.tuple(dataLocations[i].majorDeviceNumber, dataLocations[i].minorDeviceNumber));
                }
            }
            ioStats = ioStats(devicesNumbers, previous);
        }
        DiskUsage leastDiskEstimate = null;
        DiskUsage mostDiskEstimate = null;
        if (clusterInfo != null) {
            leastDiskEstimate = clusterInfo.getNodeLeastAvailableDiskUsages().get(nodeEnv.nodeId());
            mostDiskEstimate = clusterInfo.getNodeMostAvailableDiskUsages().get(nodeEnv.nodeId());
        }
        return new FsInfo(System.currentTimeMillis(), ioStats, paths, leastDiskEstimate, mostDiskEstimate);
    }

    final FsInfo.IoStats ioStats(final Set<Tuple<Integer, Integer>> devicesNumbers, final FsInfo previous) {
        try {
            final Map<Tuple<Integer, Integer>, FsInfo.DeviceStats> deviceMap = new HashMap<>();
            if (previous != null && previous.getIoStats() != null && previous.getIoStats().devicesStats != null) {
                for (int i = 0; i < previous.getIoStats().devicesStats.length; i++) {
                    FsInfo.DeviceStats deviceStats = previous.getIoStats().devicesStats[i];
                    deviceMap.put(Tuple.tuple(deviceStats.majorDeviceNumber, deviceStats.minorDeviceNumber), deviceStats);
                }
            }

            List<FsInfo.DeviceStats> devicesStats = new ArrayList<>();

            List<String> lines = readProcDiskStats();
            if (!lines.isEmpty()) {
                for (String line : lines) {
                    String fields[] = line.trim().split("\\s+");
                    final int majorDeviceNumber = Integer.parseInt(fields[0]);
                    final int minorDeviceNumber = Integer.parseInt(fields[1]);
                    if (!devicesNumbers.contains(Tuple.tuple(majorDeviceNumber, minorDeviceNumber))) {
                        continue;
                    }
                    final String deviceName = fields[2];
                    final long readsCompleted = Long.parseLong(fields[3]);
                    final long sectorsRead = Long.parseLong(fields[5]);
                    final long writesCompleted = Long.parseLong(fields[7]);
                    final long sectorsWritten = Long.parseLong(fields[9]);
                    final FsInfo.DeviceStats deviceStats =
                            new FsInfo.DeviceStats(
                                    majorDeviceNumber,
                                    minorDeviceNumber,
                                    deviceName,
                                    readsCompleted,
                                    sectorsRead,
                                    writesCompleted,
                                    sectorsWritten,
                                    deviceMap.get(Tuple.tuple(majorDeviceNumber, minorDeviceNumber)));
                    devicesStats.add(deviceStats);
                }
            }

            return new FsInfo.IoStats(devicesStats.toArray(new FsInfo.DeviceStats[devicesStats.size()]));
        } catch (Exception e) {
            // do not fail Elasticsearch if something unexpected
            // happens here
            logger.debug(
                (Supplier<?>) () -> new ParameterizedMessage(
                    "unexpected exception processing /proc/diskstats for devices {}", devicesNumbers), e);
            return null;
        }
    }

    @SuppressForbidden(reason = "read /proc/diskstats")
    List<String> readProcDiskStats() throws IOException {
        return Files.readAllLines(PathUtils.get("/proc/diskstats"));
    }

    /* See: https://bugs.openjdk.java.net/browse/JDK-8162520 */
    /**
     * Take a large value intended to be positive, and if it has overflowed,
     * return {@code Long.MAX_VALUE} instead of a negative number.
     */
    static long adjustForHugeFilesystems(long bytes) {
        if (bytes < 0) {
            return Long.MAX_VALUE;
        }
        return bytes;
    }

    public static FsInfo.Path getFSInfo(NodePath nodePath) throws IOException {
        FsInfo.Path fsPath = new FsInfo.Path();
        fsPath.path = nodePath.path.toAbsolutePath().toString();

        // NOTE: we use already cached (on node startup) FileStore and spins
        // since recomputing these once per second (default) could be costly,
        // and they should not change:
        fsPath.total = adjustForHugeFilesystems(nodePath.fileStore.getTotalSpace());
        fsPath.free = adjustForHugeFilesystems(nodePath.fileStore.getUnallocatedSpace());
        fsPath.available = adjustForHugeFilesystems(nodePath.fileStore.getUsableSpace());
        fsPath.type = nodePath.fileStore.type();
        fsPath.mount = nodePath.fileStore.toString();
        return fsPath;
    }

}
