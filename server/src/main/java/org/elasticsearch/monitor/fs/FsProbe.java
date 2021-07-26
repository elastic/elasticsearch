/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.monitor.fs;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.util.Constants;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.NodeEnvironment.NodePath;

import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class FsProbe {

    private static final Logger logger = LogManager.getLogger(FsProbe.class);

    private final NodeEnvironment nodeEnv;

    public FsProbe(NodeEnvironment nodeEnv) {
        this.nodeEnv = nodeEnv;
    }

    public FsInfo stats(FsInfo previous) throws IOException {
        if (nodeEnv.hasNodeFile() == false) {
            return new FsInfo(System.currentTimeMillis(), null, new FsInfo.Path[0]);
        }
        NodePath dataLocation = nodeEnv.nodePath();
        FsInfo.Path pathInfo = getFSInfo(dataLocation);
        FsInfo.IoStats ioStats = null;
        if (Constants.LINUX) {
            Set<Tuple<Integer, Integer>> devicesNumbers = new HashSet<>();
            if (dataLocation.majorDeviceNumber != -1 && dataLocation.minorDeviceNumber != -1) {
                devicesNumbers.add(Tuple.tuple(dataLocation.majorDeviceNumber, dataLocation.minorDeviceNumber));
            }
            ioStats = ioStats(devicesNumbers, previous);
        }
        return new FsInfo(System.currentTimeMillis(), ioStats, new FsInfo.Path[] { pathInfo });
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
            if (lines.isEmpty() == false) {
                for (String line : lines) {
                    String fields[] = line.trim().split("\\s+");
                    final int majorDeviceNumber = Integer.parseInt(fields[0]);
                    final int minorDeviceNumber = Integer.parseInt(fields[1]);
                    if (devicesNumbers.contains(Tuple.tuple(majorDeviceNumber, minorDeviceNumber)) == false) {
                        continue;
                    }
                    final String deviceName = fields[2];
                    final long readsCompleted = Long.parseLong(fields[3]);
                    final long sectorsRead = Long.parseLong(fields[5]);
                    final long writesCompleted = Long.parseLong(fields[7]);
                    final long sectorsWritten = Long.parseLong(fields[9]);
                    final long ioTime = Long.parseLong(fields[12]);
                    final FsInfo.DeviceStats deviceStats =
                            new FsInfo.DeviceStats(
                                    majorDeviceNumber,
                                    minorDeviceNumber,
                                    deviceName,
                                    readsCompleted,
                                    sectorsRead,
                                    writesCompleted,
                                    sectorsWritten,
                                    ioTime,
                                    deviceMap.get(Tuple.tuple(majorDeviceNumber, minorDeviceNumber)));
                    devicesStats.add(deviceStats);
                }
            }

            return new FsInfo.IoStats(devicesStats.toArray(new FsInfo.DeviceStats[devicesStats.size()]));
        } catch (Exception e) {
            // do not fail Elasticsearch if something unexpected
            // happens here
            logger.debug(() -> new ParameterizedMessage(
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
        fsPath.path = nodePath.path.toString();

        // NOTE: we use already cached (on node startup) FileStore and spins
        // since recomputing these once per second (default) could be costly,
        // and they should not change:
        fsPath.total = getTotal(nodePath.fileStore);
        fsPath.free = adjustForHugeFilesystems(nodePath.fileStore.getUnallocatedSpace());
        fsPath.available = adjustForHugeFilesystems(nodePath.fileStore.getUsableSpace());
        fsPath.type = nodePath.fileStore.type();
        fsPath.mount = nodePath.fileStore.toString();
        return fsPath;
    }

    public static long getTotal(FileStore fileStore) throws IOException {
        return adjustForHugeFilesystems(fileStore.getTotalSpace());
    }

}
