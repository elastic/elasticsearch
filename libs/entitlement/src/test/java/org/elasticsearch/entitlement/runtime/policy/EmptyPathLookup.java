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

/**
 * A {@link PathLookup} with no base directories, for tests that do not depend on them.
 */
final class EmptyPathLookup implements PathLookup {
    static final PathLookup INSTANCE = new EmptyPathLookup();

    private EmptyPathLookup() {}

    @Override
    public Path pidFile() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Stream<Path> getBaseDirPaths(BaseDir baseDir) {
        return Stream.empty();
    }

    @Override
    public Stream<Path> resolveSettingPaths(BaseDir baseDir, String settingName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isPathOnDefaultFilesystem(Path path) {
        return true;
    }
}
