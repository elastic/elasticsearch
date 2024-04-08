/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.cluster.local.distribution;

import org.elasticsearch.test.cluster.util.Version;

import java.nio.file.Path;

public class DefaultDistributionDescriptor implements DistributionDescriptor {
    private final Version version;
    private final boolean snapshot;
    private final Path distributionDir;
    private final DistributionType type;

    public DefaultDistributionDescriptor(Version version, boolean snapshot, Path extractedDir, DistributionType type) {
        this.version = version;
        this.snapshot = snapshot;
        this.distributionDir = extractedDir;
        this.type = type;
    }

    public Version getVersion() {
        return version;
    }

    public boolean isSnapshot() {
        return snapshot;
    }

    public Path getDistributionDir() {
        return distributionDir.resolve("elasticsearch-" + version + (snapshot ? "-SNAPSHOT" : ""));
    }

    public DistributionType getType() {
        return type;
    }

    @Override
    public String toString() {
        return "DefaultDistributionDescriptor{"
            + "version="
            + version
            + ", snapshot="
            + snapshot
            + ", distributionDir="
            + distributionDir
            + ", type="
            + type
            + '}';
    }
}
