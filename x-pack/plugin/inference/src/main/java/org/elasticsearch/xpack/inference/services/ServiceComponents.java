/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;

/**
 * A container for common components need at various levels of the inference services to instantiate their internals
 */
public record ServiceComponents(ThreadPool threadPool, ThrottlerManager throttlerManager, Settings settings) {}
