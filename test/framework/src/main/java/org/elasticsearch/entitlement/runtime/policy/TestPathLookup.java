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
import java.util.List;
import java.util.stream.Stream;

public class TestPathLookup implements PathLookup {
    final List<Path> tempDirPaths;

    public TestPathLookup(List<Path> tempDirPaths) {
        this.tempDirPaths = tempDirPaths;
    }

    @Override
    public Path pidFile() {
        return null;
    }

    @Override
    public Stream<Path> getBaseDirPaths(BaseDir baseDir) {
        return switch (baseDir) {
            case TEMP -> tempDirPaths.stream();
            default -> Stream.empty();
        };
    }

    @Override
    public Stream<Path> resolveSettingPaths(BaseDir baseDir, String settingName) {
        return Stream.empty();
    }

}
