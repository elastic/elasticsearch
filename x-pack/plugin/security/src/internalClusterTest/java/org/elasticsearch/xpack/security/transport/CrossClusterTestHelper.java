/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.transport;

import org.elasticsearch.test.InternalTestCluster;

final class CrossClusterTestHelper {

    /**
     * Returns {@link CrossClusterApiKeySignatureManager} instance from a random node of the given cluster.
     */
    static CrossClusterApiKeySignatureManager getCrossClusterApiKeySignatureManager(InternalTestCluster cluster) {
        RemoteClusterTransportInterceptor interceptor = cluster.getInstance(
            RemoteClusterTransportInterceptor.class,
            cluster.getRandomNodeName()
        );

        assert interceptor instanceof CrossClusterAccessTransportInterceptor
            : "expected cross-cluster interceptor but got " + interceptor.getClass();
        return ((CrossClusterAccessTransportInterceptor) interceptor).getCrossClusterApiKeySignatureManager();
    }

    private CrossClusterTestHelper() {
        throw new IllegalAccessError("not allowed!");
    }
}
