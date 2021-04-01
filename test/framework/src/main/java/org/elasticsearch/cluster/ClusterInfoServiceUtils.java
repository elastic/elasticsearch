/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.service.ClusterApplierService;

import java.util.concurrent.TimeUnit;

public class ClusterInfoServiceUtils {

    private static final Logger logger = LogManager.getLogger(ClusterInfoServiceUtils.class);

    public static ClusterInfo refresh(InternalClusterInfoService internalClusterInfoService) {
        logger.trace("refreshing cluster info");
        final PlainActionFuture<ClusterInfo> future = new PlainActionFuture<>(){
            @Override
            protected boolean blockingAllowed() {
                // In tests we permit blocking the applier thread here so that we know a followup reroute isn't working with stale data.
                return Thread.currentThread().getName().contains(ClusterApplierService.CLUSTER_UPDATE_THREAD_NAME)
                        || super.blockingAllowed();
            }
        };
        internalClusterInfoService.refreshAsync(future);
        try {
            return future.actionGet(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }
}
