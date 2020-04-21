/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.plugin;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.async.AsyncTaskIndexService;
import org.elasticsearch.xpack.core.async.AsyncTaskMaintenanceService;

public class AsyncEqlSearchMaintenanceService extends AsyncTaskMaintenanceService {

    public static final Setting<TimeValue> EQL_ASYNC_SEARCH_CLEANUP_INTERVAL_SETTING =
        Setting.timeSetting("eql.async_search.index_cleanup_interval", TimeValue.timeValueHours(1), Setting.Property.NodeScope);

    AsyncEqlSearchMaintenanceService(String localNodeId,
                                     Settings nodeSettings,
                                     ThreadPool threadPool,
                                     AsyncTaskIndexService<?> indexService) {
        super(EqlPlugin.INDEX, localNodeId, threadPool, indexService, EQL_ASYNC_SEARCH_CLEANUP_INTERVAL_SETTING.get(nodeSettings));
    }
}
