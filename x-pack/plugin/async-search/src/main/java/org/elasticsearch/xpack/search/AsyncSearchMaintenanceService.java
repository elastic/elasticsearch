/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.search;

import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.async.AsyncTaskIndexService;
import org.elasticsearch.xpack.core.async.AsyncTaskMaintenanceService;

public class AsyncSearchMaintenanceService extends AsyncTaskMaintenanceService {

    AsyncSearchMaintenanceService(String localNodeId,
                                  ThreadPool threadPool,
                                  AsyncTaskIndexService<?> indexService,
                                  TimeValue delay) {
        super(AsyncSearch.INDEX, localNodeId, threadPool, indexService, delay);
    }
}
