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

package org.elasticsearch.test.cluster.stateless.local;

import org.apache.commons.lang3.RandomStringUtils;
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
            spec.getNodes()
                .stream()
                .map(s -> new Node(baseWorkingDir, distributionResolver, s, RandomStringUtils.randomAlphabetic(7), true))
                .collect(Collectors.toList())
        );
    }
}
