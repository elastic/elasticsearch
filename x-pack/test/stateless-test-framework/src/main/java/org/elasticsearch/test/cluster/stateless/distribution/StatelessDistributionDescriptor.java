/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
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
