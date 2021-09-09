/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.benchmark;

import org.elasticsearch.client.benchmark.metrics.SampleRecorder;

public interface BenchmarkTask {
    void setUp(SampleRecorder sampleRecorder) throws Exception;

    void run() throws Exception;

    void tearDown() throws Exception;
}
