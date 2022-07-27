/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.job.process.autodetect;

import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.xpack.ml.job.process.ProcessWorkerExecutorService;

public class AutodetectWorkerExecutorService extends ProcessWorkerExecutorService {

    public AutodetectWorkerExecutorService(ThreadContext contextHolder) {
        super(contextHolder, "autodetect", 100);
    }
}
