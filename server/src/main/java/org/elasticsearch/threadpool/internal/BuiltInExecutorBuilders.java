/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.threadpool.internal;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ExecutorBuilder;

import java.util.Map;

public interface BuiltInExecutorBuilders {
    @SuppressWarnings("rawtypes")
    Map<String, ExecutorBuilder> getBuilders(Settings settings, int allocatedProcessors);
}
