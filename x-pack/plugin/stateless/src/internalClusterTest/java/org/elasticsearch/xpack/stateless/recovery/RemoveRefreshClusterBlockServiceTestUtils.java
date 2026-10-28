/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery;

import org.elasticsearch.index.Index;

import java.util.Set;

public final class RemoveRefreshClusterBlockServiceTestUtils {

    private RemoveRefreshClusterBlockServiceTestUtils() {}

    public static Set<Index> blockedIndices(RemoveRefreshClusterBlockService service) {
        return service.blockedIndices();
    }
}
