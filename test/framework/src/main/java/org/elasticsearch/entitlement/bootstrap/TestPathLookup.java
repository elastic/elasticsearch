/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.bootstrap;

import org.apache.lucene.tests.mockfile.FilterFileSystem;
import org.elasticsearch.entitlement.runtime.policy.PathLookup;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import static org.elasticsearch.entitlement.runtime.policy.PathLookup.BaseDir.TEMP;

class TestPathLookup implements PathLookup {
    private final Map<BaseDir, Collection<Path>> baseDirPaths;

    TestPathLookup(Path tempDir) {
        baseDirPaths = new ConcurrentHashMap<>();
        baseDirPaths.put(TEMP, List.of(tempDir));
    }

    @Override
    public Path pidFile() {
        return null;
    }

    @Override
    public Stream<Path> getBaseDirPaths(BaseDir baseDir) {
        return baseDirPaths.getOrDefault(baseDir, List.of()).stream();
    }

    @Override
    public Stream<Path> resolveSettingPaths(BaseDir baseDir, String settingName) {
        return Stream.empty();
    }

    @Override
    public boolean isPathOnDefaultFilesystem(Path path) {
        var fileSystem = path.getFileSystem();
        if (fileSystem.getClass() != DEFAULT_FILESYSTEM_CLASS) {
            while (fileSystem instanceof FilterFileSystem ffs) {
                fileSystem = ffs.getDelegate();
            }
        }
        return fileSystem.getClass() == DEFAULT_FILESYSTEM_CLASS;
    }

    void reset() {
        baseDirPaths.keySet().retainAll(List.of(TEMP));
    }

    void add(BaseDir baseDir, Path... paths) {
        baseDirPaths.compute(baseDir, baseDirModifier(Collection::add, paths));
    }

    void remove(BaseDir baseDir, Path... paths) {
        baseDirPaths.compute(baseDir, baseDirModifier(Collection::remove, paths));
    }

    // This must allow for duplicate paths between nodes, the config dir for instance is shared across all nodes.
    private static BiFunction<BaseDir, Collection<Path>, Collection<Path>> baseDirModifier(
        BiConsumer<Collection<Path>, Path> operation,
        Path... updates
    ) {
        // always return a new unmodifiable copy
        return (BaseDir baseDir, Collection<Path> paths) -> {
            paths = paths == null ? new ArrayList<>() : new ArrayList<>(paths);
            for (Path update : updates) {
                operation.accept(paths, update);
            }
            return Collections.unmodifiableCollection(paths);
        };
    }
}
