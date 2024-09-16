/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.cluster.local;

import org.elasticsearch.test.cluster.local.distribution.DistributionResolver;

import java.nio.file.Path;

public class DefaultLocalClusterFactory extends AbstractLocalClusterFactory<LocalClusterSpec, DefaultLocalClusterHandle> {
    private final DistributionResolver distributionResolver;

    public DefaultLocalClusterFactory(DistributionResolver distributionResolver) {
        super(distributionResolver);
        this.distributionResolver = distributionResolver;
    }

    protected DefaultLocalClusterHandle createHandle(Path baseWorkingDir, LocalClusterSpec spec) {
        return new DefaultLocalClusterHandle(
            spec.getName(),
            spec.getNodes().stream().map(s -> new Node(baseWorkingDir, distributionResolver, s)).toList()
        );
    }
}
