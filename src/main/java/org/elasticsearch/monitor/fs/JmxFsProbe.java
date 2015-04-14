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
import org.elasticsearch.env.NodeEnvironment.NodePath;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;

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
        NodePath[] dataLocations = nodeEnv.nodePaths();
        FsStats.Info[] infos = new FsStats.Info[dataLocations.length];
        for (int i = 0; i < dataLocations.length; i++) {
            infos[i] = getFSInfo(dataLocations[i]);
        }
        return new FsStats(System.currentTimeMillis(), infos);
    }

    public static FsStats.Info getFSInfo(NodePath nodePath) throws IOException {
        FsStats.Info info = new FsStats.Info();
        info.path = nodePath.path.toAbsolutePath().toString();

        // NOTE: we use already cached (on node startup) FileStore and spins
        // since recomputing these once per second (default) could be costly,
        // and they should not change:
        info.total = nodePath.fileStore.getTotalSpace();
        info.free = nodePath.fileStore.getUnallocatedSpace();
        info.available = nodePath.fileStore.getUsableSpace();
        info.type = nodePath.fileStore.type();
        info.mount = nodePath.fileStore.toString();
        info.spins = nodePath.spins;
        return info;
    }
}
