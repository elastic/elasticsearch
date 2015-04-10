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

import com.google.common.collect.Maps;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.NodeEnvironment.NodePath;
import org.elasticsearch.monitor.sigar.SigarService;
import org.hyperic.sigar.FileSystem;
import org.hyperic.sigar.FileSystemMap;
import org.hyperic.sigar.FileSystemUsage;
import org.hyperic.sigar.Sigar;
import org.hyperic.sigar.SigarException;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

public class SigarFsProbe extends AbstractComponent implements FsProbe {

    private final NodeEnvironment nodeEnv;

    private final SigarService sigarService;

    private Map<Path, FileSystem> fileSystems = Maps.newHashMap();

    @Inject
    public SigarFsProbe(Settings settings, NodeEnvironment nodeEnv, SigarService sigarService) {
        super(settings);
        this.nodeEnv = nodeEnv;
        this.sigarService = sigarService;
    }

    @Override
    public synchronized FsStats stats() {
        if (!nodeEnv.hasNodeFile()) {
            return new FsStats(System.currentTimeMillis(), new FsStats.Info[0]);
        }
        NodePath[] nodePaths = nodeEnv.nodePaths();
        FsStats.Info[] infos = new FsStats.Info[nodePaths.length];
        for (int i = 0; i < nodePaths.length; i++) {
            NodePath nodePath = nodePaths[i];
            Path dataLocation = nodePath.path;

            FsStats.Info info = new FsStats.Info();
            info.path = dataLocation.toAbsolutePath().toString();

            try {
                FileSystem fileSystem = fileSystems.get(dataLocation);
                Sigar sigar = sigarService.sigar();
                if (fileSystem == null) {
                    FileSystemMap fileSystemMap = sigar.getFileSystemMap();
                    if (fileSystemMap != null) {
                        fileSystem = fileSystemMap.getMountPoint(dataLocation.toAbsolutePath().toString());
                        fileSystems.put(dataLocation, fileSystem);
                    }
                }
                if (fileSystem != null) {
                    info.mount = fileSystem.getDirName();
                    info.dev = fileSystem.getDevName();
                    info.type = fileSystem.getSysTypeName();
                    info.spins = nodePath.spins;

                    FileSystemUsage fileSystemUsage = sigar.getFileSystemUsage(fileSystem.getDirName());
                    if (fileSystemUsage != null) {
                        // total/free/available seem to be in megabytes?
                        info.total = fileSystemUsage.getTotal() * 1024;
                        info.free = fileSystemUsage.getFree() * 1024;
                        info.available = fileSystemUsage.getAvail() * 1024;
                        info.diskReads = fileSystemUsage.getDiskReads();
                        info.diskWrites = fileSystemUsage.getDiskWrites();
                        info.diskReadBytes = fileSystemUsage.getDiskReadBytes();
                        info.diskWriteBytes = fileSystemUsage.getDiskWriteBytes();
                        info.diskQueue = fileSystemUsage.getDiskQueue();
                        info.diskServiceTime = fileSystemUsage.getDiskServiceTime();
                    }
                }
            } catch (SigarException e) {
                // failed...
            }

            infos[i] = info;
        }

        return new FsStats(System.currentTimeMillis(), infos);
    }
}
