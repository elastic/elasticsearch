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

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.NodeEnvironment.NodePath;
import org.elasticsearch.monitor.fs.FsInfo;

public class FsProbe extends AbstractComponent {

    private final NodeEnvironment nodeEnv;
    private final Map<Path, ByteSizeValue> quotas = new ConcurrentHashMap<>();

    public FsProbe(Settings settings, NodeEnvironment nodeEnv) {
        super(settings);
        this.nodeEnv = nodeEnv;
    }

    public FsInfo stats() throws IOException {
        if (!nodeEnv.hasNodeFile()) {
            return new FsInfo(System.currentTimeMillis(), new FsInfo.Path[0]);
        }
        NodePath[] dataLocations = nodeEnv.nodePaths();
        FsInfo.Path[] paths = new FsInfo.Path[dataLocations.length];
        for (int i = 0; i < dataLocations.length; i++) {
            paths[i] = getQuotaAwareFSInfo(dataLocations[i]);
        }
        return new FsInfo(System.currentTimeMillis(), paths);
    }

    public FsInfo.Path getQuotaAwareFSInfo(NodePath nodePath) throws IOException {
        Optional<Path> closestMatchingQuota = quotas.keySet().stream().filter((path) -> nodePath.path.startsWith(path))
                .max((a, b) -> a.getNameCount() - b.getNameCount());
        FsInfo.Path fsInfo = getFSInfoIgnoringQuota(nodePath);
        if (!closestMatchingQuota.isPresent()) {
            logger.trace("No quota for path: [{}]", nodePath.path);
            return fsInfo;
        } else {
            ByteSizeValue limit = quotas.get(closestMatchingQuota.get());
            logger.trace("Limiting capacity in path: [{}] to quota: [{}]", nodePath.path, limit);
            long total = min(fsInfo.getTotal(), limit).bytes();
            long remainingInQuota = Math.max(limit.bytes() - size(nodePath.path), 0);
            long free = Math.min(fsInfo.getFree().bytes(), remainingInQuota);
            long available = Math.min(fsInfo.getAvailable().bytes(), remainingInQuota);
            FsInfo.Path limitedPath = new FsInfo.Path(fsInfo.getPath(), fsInfo.getMount(), total, free, available);
            limitedPath.type = fsInfo.type;
            return limitedPath;
        }
    }

    static ByteSizeValue min(ByteSizeValue a, ByteSizeValue b) {
        return a.bytes() < b.bytes() ? a : b;
    }

    /**
     * Attempts to calculate the size of a file or directory.
     *
     * <p>
     * Since the operation is non-atomic, the returned value may be inaccurate.
     * However, this method is quick and does its best.
     */
    private long size(Path path) {
        final AtomicLong size = new AtomicLong(0);

        try {
            SimpleFileVisitor<Path> simpleFileVisitor = new SimpleFileVisitor<Path>() {

                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                    size.addAndGet(attrs.size());
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFileFailed(Path file, IOException exc) {
                    logger.warn("Unable to check size on file: [{}]", file, exc);
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult postVisitDirectory(Path dir, IOException exc) {
                    if (exc != null) {
                        logger.warn("Unable to check size on: [{}]", dir, exc);
                    }
                    return FileVisitResult.CONTINUE;
                }
            };
            Files.walkFileTree(path, simpleFileVisitor);
        } catch (IOException e) {
            throw new AssertionError("walkFileTree will not throw IOException if the FileVisitor does not");
        }

        return size.get();
    }

    public static FsInfo.Path getFSInfoIgnoringQuota(NodePath nodePath) throws IOException {
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

    public void setQuota(Path path, ByteSizeValue limit) {
        if (limit == null) {
            quotas.remove(path);
        } else {
            quotas.put(path, limit);
        }
    }
}
