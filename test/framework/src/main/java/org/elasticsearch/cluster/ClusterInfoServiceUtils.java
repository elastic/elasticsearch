/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
