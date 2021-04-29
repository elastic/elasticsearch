/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices;

import org.elasticsearch.threadpool.ThreadPool;

import java.util.Set;
import java.util.stream.Collectors;

/**
 * Some operations need to use different executors for different index patterns.
 * Specifically, some operations on system indices are considered critical and
 * should use the "system_critical_read" or "system_critical_write" thread pools
 * rather than the "system_read" or "system_write" thread pools.
 */
public class ExecutorSelectorService {
    private final SystemIndices systemIndices;
    private final Set<SystemIndexDescriptor> criticalSystemIndexDescriptors;

    public ExecutorSelectorService(SystemIndices systemIndices) {
        this.systemIndices = systemIndices;
        this.criticalSystemIndexDescriptors = systemIndices.getSystemIndexDescriptors().stream()
            .filter(SystemIndexDescriptor::isInternal) // TODO[wrb]: should be "isCritical"
            .collect(Collectors.toSet());
    }

    public String getReadExecutor(String indexName) {
        for (SystemIndexDescriptor descriptor : criticalSystemIndexDescriptors) {
            if (descriptor.matchesIndexPattern(indexName)) {
                return ThreadPool.Names.SYSTEM_CRITICAL_READ;
            }
        }

        if (systemIndices.isSystemIndex(indexName)) {
            return ThreadPool.Names.SYSTEM_READ;
        }
        return ThreadPool.Names.SEARCH;
    }

    public String getWriteExecutor(String indexName) {
        for (SystemIndexDescriptor descriptor : criticalSystemIndexDescriptors) {
            if (descriptor.matchesIndexPattern(indexName)) {
                return ThreadPool.Names.SYSTEM_CRITICAL_WRITE;
            }
        }

        if (systemIndices.isSystemIndex(indexName)) {
            return ThreadPool.Names.SYSTEM_WRITE;
        }
        return ThreadPool.Names.WRITE;
    }
}
