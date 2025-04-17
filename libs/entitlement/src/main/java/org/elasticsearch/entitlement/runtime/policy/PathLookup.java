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
import java.util.stream.Stream;

// TODO: (jack) new a need interface, but can still be a record
// interface should be a single method that takes in an enum and returns
// a stream of paths
public interface PathLookup {
    enum BaseDir {
        HOME,
        CONFIG,
        DATA,
        SHARED_REPO,
        LIB,
        MODULES,
        PLUGINS,
        LOGS,
        TEMP,
        PID
    }

    Stream<Path> getBaseDirPaths(BaseDir baseDir);

    Stream<Path> resolveRelativePaths(BaseDir baseDir, Path relativePath);

    Stream<Path> resolveSettingPaths(BaseDir baseDir, String settingName);
}
