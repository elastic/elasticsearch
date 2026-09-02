/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.test.cluster.stateless.local;

import org.elasticsearch.test.cluster.local.AbstractLocalClusterFactory;
import org.elasticsearch.test.cluster.local.LocalClusterSpec;
import org.elasticsearch.test.cluster.local.distribution.DistributionResolver;

import java.nio.file.Path;
import java.util.stream.Collectors;

public class StatelessLocalClusterFactory extends AbstractLocalClusterFactory<LocalClusterSpec, StatelessLocalClusterHandle> {
    private final DistributionResolver distributionResolver;

    public StatelessLocalClusterFactory(DistributionResolver distributionResolver) {
        super(distributionResolver);
        this.distributionResolver = distributionResolver;
    }

    @Override
    protected StatelessLocalClusterHandle createHandle(Path baseWorkingDir, LocalClusterSpec spec) {
        return new StatelessLocalClusterHandle(
            spec.getName(),
            baseWorkingDir,
            distributionResolver,
            spec.getNodes().stream().map(s -> new StatelessNode(baseWorkingDir, distributionResolver, s)).collect(Collectors.toList())
        );
    }
}
