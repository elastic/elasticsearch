/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.test.cluster.stateless.distribution;

import org.elasticsearch.test.cluster.local.distribution.DistributionDescriptor;
import org.elasticsearch.test.cluster.local.distribution.DistributionResolver;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.util.Version;

public class StatelessDistributionResolver implements DistributionResolver {
    private final DistributionResolver delegate;

    public StatelessDistributionResolver(DistributionResolver delegate) {
        this.delegate = delegate;
    }

    @Override
    public DistributionDescriptor resolve(Version version, DistributionType type) {
        return new StatelessDistributionDescriptor(delegate.resolve(version, type));
    }
}
