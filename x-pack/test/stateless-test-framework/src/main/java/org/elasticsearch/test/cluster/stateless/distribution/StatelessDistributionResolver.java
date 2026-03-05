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
