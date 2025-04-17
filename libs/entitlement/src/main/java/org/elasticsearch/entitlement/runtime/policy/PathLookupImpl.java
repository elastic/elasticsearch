/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.runtime.policy;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

public record PathLookupImpl(
    Path homeDir,
    Path configDir,
    Path[] dataDirs,
    Path[] sharedRepoDirs,
    Path libDir,
    Path modulesDir,
    Path pluginsDir,
    Path logsDir,
    Path tempDir,
    Path pidFile,
    Function<String, Stream<String>> settingResolver
) implements PathLookup {

    public PathLookupImpl {
        requireNonNull(homeDir);
        requireNonNull(dataDirs);
        if (dataDirs.length == 0) {
            throw new IllegalArgumentException("must provide at least one data directory");
        }
        requireNonNull(sharedRepoDirs);
        requireNonNull(configDir);
        requireNonNull(libDir);
        requireNonNull(modulesDir);
        requireNonNull(pluginsDir);
        requireNonNull(logsDir);
        requireNonNull(tempDir);
        requireNonNull(settingResolver);
    }

    @Override
    public Stream<Path> getBaseDirPaths(BaseDir baseDir) {
        if (baseDir == BaseDir.HOME) {
            return Stream.of(homeDir);
        } else if (baseDir == BaseDir.DATA) {
            return Arrays.stream(dataDirs);
        } else if (baseDir == BaseDir.SHARED_REPO) {
            return Arrays.stream(sharedRepoDirs);
        } else if (baseDir == BaseDir.CONFIG) {
            return Stream.of(configDir);
        } else if (baseDir == BaseDir.LIB) {
            return Stream.of(libDir);
        } else if (baseDir == BaseDir.MODULES) {
            return Stream.of(modulesDir);
        } else if (baseDir == BaseDir.PLUGINS) {
            return Stream.of(pluginsDir);
        } else if (baseDir == BaseDir.LOGS) {
            return Stream.of(logsDir);
        } else if (baseDir == BaseDir.TEMP) {
            return Stream.of(tempDir);
        } else if (baseDir == BaseDir.PID) {
            return pidFile == null ? null : Stream.of(pidFile);
        } else {
            throw new IllegalStateException("unknown base dir [" + baseDir + "]");
        }
    }

    @Override
    public Stream<Path> resolveRelativePaths(BaseDir baseDir, Path relativePath) {
        if (baseDir == BaseDir.HOME) {
            return Stream.of(homeDir.resolve(relativePath));
        } else if (baseDir == BaseDir.DATA) {
            return Arrays.stream(dataDirs).map(dataDirPath -> dataDirPath.resolve(relativePath));
        } else if (baseDir == BaseDir.SHARED_REPO) {
            return Arrays.stream(sharedRepoDirs).map(sharedRepoDir -> sharedRepoDir.resolve(relativePath));
        } else if (baseDir == BaseDir.CONFIG) {
            return Stream.of(configDir.resolve(relativePath));
        } else if (baseDir == BaseDir.LIB) {
            return Stream.of(libDir.resolve(relativePath));
        } else if (baseDir == BaseDir.MODULES) {
            return Stream.of(modulesDir.resolve(relativePath));
        } else if (baseDir == BaseDir.PLUGINS) {
            return Stream.of(pluginsDir.resolve(relativePath));
        } else if (baseDir == BaseDir.LOGS) {
            return Stream.of(logsDir.resolve(relativePath));
        } else if (baseDir == BaseDir.TEMP) {
            return Stream.of(tempDir.resolve(relativePath));
        } else if (baseDir == BaseDir.PID) {
            return pidFile == null ? null : Stream.of(pidFile.resolve(relativePath));
        } else {
            throw new IllegalStateException("unknown base dir [" + baseDir + "]");
        }
    }

    @Override
    public Stream<Path> resolveSettingPaths(BaseDir baseDir, String settingName) {
        Stream<Path> relativePaths = settingResolver.apply(settingName)
            .filter(s -> s.toLowerCase(Locale.ROOT).startsWith("https://") == false)
            .distinct()
            .map(Path::of);
        if (baseDir == BaseDir.HOME) {
            return relativePaths.map(homeDir::resolve);
        } else if (baseDir == BaseDir.DATA) {
            return relativePathsCombination(dataDirs, relativePaths);
        } else if (baseDir == BaseDir.SHARED_REPO) {
            return relativePathsCombination(sharedRepoDirs, relativePaths);
        } else if (baseDir == BaseDir.CONFIG) {
            return relativePaths.map(configDir::resolve);
        } else if (baseDir == BaseDir.LIB) {
            return relativePaths.map(libDir::resolve);
        } else if (baseDir == BaseDir.MODULES) {
            return relativePaths.map(modulesDir::resolve);
        } else if (baseDir == BaseDir.PLUGINS) {
            return relativePaths.map(pluginsDir::resolve);
        } else if (baseDir == BaseDir.LOGS) {
            return relativePaths.map(logsDir::resolve);
        } else if (baseDir == BaseDir.TEMP) {
            return relativePaths.map(tempDir::resolve);
        } else if (baseDir == BaseDir.PID) {
            return relativePaths.map(pidFile::resolve);
        } else {
            throw new IllegalStateException("unknown base dir [" + baseDir + "]");
        }
    }

    private static Stream<Path> relativePathsCombination(Path[] baseDirs, Stream<Path> relativePaths) {
        // multiple base dirs are a pain...we need the combination of the base dirs and relative paths
        List<Path> paths = new ArrayList<>();
        for (var relativePath : relativePaths.toList()) {
            for (var dataDir : baseDirs) {
                paths.add(dataDir.resolve(relativePath));
            }
        }
        return paths.stream();
    }
}
