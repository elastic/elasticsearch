/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.cluster.local;

import org.elasticsearch.test.cluster.local.distribution.DistributionResolver;

import java.nio.file.Path;

public class LocalClusterFactory extends AbstractLocalClusterFactory<LocalClusterSpec, LocalClusterHandle> {
    private final DistributionResolver distributionResolver;

    public LocalClusterFactory(DistributionResolver distributionResolver) {
        super(distributionResolver);
        this.distributionResolver = distributionResolver;
    }

    protected LocalClusterHandle createHandle(Path baseWorkingDir, LocalClusterSpec spec) {
        return new LocalClusterHandle(
            spec.getName(),
            spec.getNodes().stream().map(s -> new Node(baseWorkingDir, distributionResolver, s)).toList()
        );
    }
}
