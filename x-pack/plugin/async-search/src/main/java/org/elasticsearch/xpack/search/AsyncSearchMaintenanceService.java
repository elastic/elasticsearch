/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.search;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.async.AsyncTaskIndexService;
import org.elasticsearch.xpack.core.async.AsyncTaskMaintenanceService;

public class AsyncSearchMaintenanceService extends AsyncTaskMaintenanceService {

    /**
     * Controls the interval at which the cleanup is scheduled.
     * Defaults to 1h. It is an undocumented/expert setting that
     * is mainly used by integration tests to make the garbage
     * collection of search responses more reactive.
     */
    public static final Setting<TimeValue> ASYNC_SEARCH_CLEANUP_INTERVAL_SETTING =
        Setting.timeSetting("async_search.index_cleanup_interval", TimeValue.timeValueHours(1), Setting.Property.NodeScope);

    AsyncSearchMaintenanceService(ClusterService clusterService,
                                  String localNodeId,
                                  Settings nodeSettings,
                                  ThreadPool threadPool,
                                  AsyncTaskIndexService<?> indexService) {
        super(clusterService, AsyncSearch.INDEX, localNodeId, threadPool, indexService,
            ASYNC_SEARCH_CLEANUP_INTERVAL_SETTING.get(nodeSettings));
    }
}
