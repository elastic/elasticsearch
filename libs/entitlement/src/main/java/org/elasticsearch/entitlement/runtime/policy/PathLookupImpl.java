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
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

public record PathLookupImpl(
    Path homeDir,
    Path[] dataDirs,
    Path[] sharedRepoDirs,
    Path configDir,
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
    public Stream<Path> resolvePaths(BaseDir baseDir) {
        if (baseDir == BaseDir.HOME) {
            return Stream.of(homeDir);
        } else if (baseDir == BaseDir.DATA) {
            return Stream.of(dataDirs);
        } else if (baseDir == BaseDir.SHARED_REPO) {
            return Stream.of(sharedRepoDirs);
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
            return Stream.of(pidFile);
        } else {
            throw new IllegalStateException("unknown base dir [" + baseDir + "]");
        }
    }

    @Override
    public Stream<String> resolveSetting(String name) {
        return settingResolver.apply(name);
    }
}
