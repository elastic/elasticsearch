/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;

import static org.elasticsearch.xpack.inference.logging.ThrottlerManagerTests.mockThrottlerManager;

public class ServiceComponentsTests extends ESTestCase {
    public static ServiceComponents createWithEmptySettings(ThreadPool threadPool) {
        return new ServiceComponents(threadPool, mockThrottlerManager(), Settings.EMPTY);
    }
}
