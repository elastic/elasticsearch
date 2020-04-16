/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.plugin;

import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.async.AsyncTaskIndexService;
import org.elasticsearch.xpack.core.async.AsyncTaskMaintenanceService;
import org.elasticsearch.xpack.core.eql.action.AsyncEqlSearchResponse;

public class AsyncEqlSearchMaintenanceService extends AsyncTaskMaintenanceService<AsyncEqlSearchResponse> {

    AsyncEqlSearchMaintenanceService(String localNodeId,
                                     ThreadPool threadPool,
                                     AsyncTaskIndexService<AsyncEqlSearchResponse> indexService,
                                     TimeValue delay) {
        super(EqlPlugin.INDEX, localNodeId, threadPool, indexService, delay);
    }
}
