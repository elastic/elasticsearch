/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.test.cluster.stateless.distribution;

import org.elasticsearch.test.cluster.local.distribution.DistributionDescriptor;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.util.Version;

import java.nio.file.Files;
import java.nio.file.Path;

public class StatelessDistributionDescriptor implements DistributionDescriptor {
    private final DistributionDescriptor delegate;

    public StatelessDistributionDescriptor(DistributionDescriptor delegate) {
        this.delegate = delegate;
    }

    @Override
    public Version getVersion() {
        return delegate.getVersion();
    }

    @Override
    public boolean isSnapshot() {
        return delegate.isSnapshot();
    }

    @Override
    public Path getDistributionDir() {
        Path defaultDir = delegate.getDistributionDir();
        if (Files.exists(defaultDir)) {
            // The upstream distribution uses the standard elasticsearch-<version> directory layout.
            return defaultDir;
        }
        // Serverless distributions do not include a version in the path.
        Path serverlessDir = defaultDir.getParent().resolve("elasticsearch");
        if (Files.exists(serverlessDir) == false) {
            throw new IllegalStateException("Could not locate elasticsearch distribution. Tried: " + defaultDir + " and " + serverlessDir);
        }
        return serverlessDir;
    }

    @Override
    public DistributionType getType() {
        return delegate.getType();
    }

    @Override
    public String toString() {
        return "StatelessDistributionDescriptor{"
            + "distributionDir="
            + getDistributionDir()
            + ", version="
            + getVersion()
            + ", snapshot="
            + isSnapshot()
            + ", type="
            + getType()
            + '}';
    }
}
