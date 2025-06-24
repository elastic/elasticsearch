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
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

/**
 * Standard manager for resolving known paths.
 */
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
        return switch (baseDir) {
            case USER_HOME -> Stream.of(homeDir);
            case DATA -> Arrays.stream(dataDirs);
            case SHARED_REPO -> Arrays.stream(sharedRepoDirs);
            case CONFIG -> Stream.of(configDir);
            case LIB -> Stream.of(libDir);
            case MODULES -> Stream.of(modulesDir);
            case PLUGINS -> Stream.of(pluginsDir);
            case LOGS -> Stream.of(logsDir);
            case TEMP -> Stream.of(tempDir);
        };
    }

    @Override
    public Stream<Path> resolveSettingPaths(BaseDir baseDir, String settingName) {
        List<Path> relativePaths = settingResolver.apply(settingName)
            .filter(s -> s.toLowerCase(Locale.ROOT).startsWith("https://") == false)
            .distinct()
            .map(Path::of)
            .toList();
        return getBaseDirPaths(baseDir).flatMap(path -> relativePaths.stream().map(path::resolve));
    }
}
