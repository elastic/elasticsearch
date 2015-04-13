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

import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.NodeEnvironment;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 */
public class JmxFsProbe extends AbstractComponent implements FsProbe {

    private final NodeEnvironment nodeEnv;

    @Inject
    public JmxFsProbe(Settings settings, NodeEnvironment nodeEnv) {
        super(settings);
        this.nodeEnv = nodeEnv;
    }

    @Override
    public FsStats stats() throws IOException {
        if (!nodeEnv.hasNodeFile()) {
            return new FsStats(System.currentTimeMillis(), new FsStats.Info[0]);
        }
        Path[] dataLocations = nodeEnv.nodeDataPaths();
        FsStats.Info[] infos = new FsStats.Info[dataLocations.length];
        for (int i = 0; i < dataLocations.length; i++) {
            Path dataLocation = dataLocations[i];
            FsStats.Info info = new FsStats.Info();
            FileStore fileStore = Files.getFileStore(dataLocation);
            info.path = dataLocation.toAbsolutePath().toString();
            info.total = fileStore.getTotalSpace();
            info.free = fileStore.getUnallocatedSpace();
            info.available = fileStore.getUsableSpace();
            infos[i] = info;
        }
        return new FsStats(System.currentTimeMillis(), infos);
    }
}
