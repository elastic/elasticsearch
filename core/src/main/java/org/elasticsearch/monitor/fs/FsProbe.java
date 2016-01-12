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

import org.apache.lucene.util.Constants;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.NodeEnvironment.NodePath;

import java.io.IOException;
import java.nio.file.Files;
import java.util.List;

public class FsProbe extends AbstractComponent {

    private final NodeEnvironment nodeEnv;

    public FsProbe(Settings settings, NodeEnvironment nodeEnv) {
        super(settings);
        this.nodeEnv = nodeEnv;
    }

    public FsInfo stats() throws IOException {
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
            ioStats = readProcSelfIo("/proc/self/io");
        }
        return new FsInfo(System.currentTimeMillis(), ioStats, paths);
    }

    @SuppressForbidden(reason = "access /proc")
    private static FsInfo.IoStats readProcSelfIo(String procSelfIo) {
        try {
            List<String> lines = Files.readAllLines(PathUtils.get(procSelfIo));
            if (!lines.isEmpty()) {
                long[] values = new long[6];
                for (int i = 0; i < 6; i++) {
                    values[i] = Long.parseLong(lines.get(i).split("\\s+")[1]);
                }
                return new FsInfo.IoStats(values[0], values[1], values[2], values[3], values[4], values[5]);
            }
        } catch (IOException e) {
            // do not fail Elasticsearch if something unexpected
            // happens here
        }
        return null;
    }

    public static FsInfo.Path getFSInfo(NodePath nodePath) throws IOException {
        FsInfo.Path fsPath = new FsInfo.Path();
        fsPath.path = nodePath.path.toAbsolutePath().toString();

        // NOTE: we use already cached (on node startup) FileStore and spins
        // since recomputing these once per second (default) could be costly,
        // and they should not change:
        fsPath.total = nodePath.fileStore.getTotalSpace();
        fsPath.free = nodePath.fileStore.getUnallocatedSpace();
        fsPath.available = nodePath.fileStore.getUsableSpace();
        fsPath.type = nodePath.fileStore.type();
        fsPath.mount = nodePath.fileStore.toString();
        fsPath.spins = nodePath.spins;
        return fsPath;
    }
}
