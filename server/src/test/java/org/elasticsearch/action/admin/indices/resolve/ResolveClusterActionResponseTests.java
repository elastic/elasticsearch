/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.resolve;

import org.elasticsearch.Build;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.transport.RemoteClusterAware;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

public class ResolveClusterActionResponseTests extends AbstractWireSerializingTestCase<ResolveClusterActionResponse> {

    @Override
    protected ResolveClusterActionResponse createTestInstance() {
        return new ResolveClusterActionResponse(randomResolveClusterInfoMap(() -> randomResolveClusterInfo()));
    }

    private Map<String, ResolveClusterInfo> randomResolveClusterInfoMap(Supplier<ResolveClusterInfo> rcInfoSupplier) {
        Map<String, ResolveClusterInfo> infoMap = new HashMap<>();
        int numClusters = randomIntBetween(0, 100);
        if (randomBoolean() || numClusters == 0) {
            infoMap.put(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY, rcInfoSupplier.get());
        }
        for (int i = 0; i < numClusters; i++) {
            infoMap.put("remote_" + i, rcInfoSupplier.get());
        }
        return infoMap;
    }

    /**
     * Useful for the mutateInstance method to ensure that no the new ResolveClusterInfo will not
     * match the original one.
     *
     * @return version of ResolveClusterInfo that has randomized error strings
     */
    static ResolveClusterInfo randomResolveClusterInfoWithErrorString() {
        return new ResolveClusterInfo(randomBoolean(), randomBoolean(), randomAlphaOfLength(125));
    }

    static ResolveClusterInfo randomResolveClusterInfo() {
        int val = randomIntBetween(1, 3);
        return switch (val) {
            case 1 -> new ResolveClusterInfo(false, randomBoolean());
            case 2 -> new ResolveClusterInfo(randomBoolean(), randomBoolean(), randomAlphaOfLength(15));
            case 3 -> new ResolveClusterInfo(randomBoolean(), randomBoolean(), randomBoolean(), Build.current());
            default -> throw new UnsupportedOperationException("should not get here");
        };
    }

    @Override
    protected Writeable.Reader<ResolveClusterActionResponse> instanceReader() {
        return ResolveClusterActionResponse::new;
    }

    @Override
    protected ResolveClusterActionResponse mutateInstance(ResolveClusterActionResponse response) {
        return new ResolveClusterActionResponse(randomResolveClusterInfoMap(() -> randomResolveClusterInfoWithErrorString()));
    }
}
