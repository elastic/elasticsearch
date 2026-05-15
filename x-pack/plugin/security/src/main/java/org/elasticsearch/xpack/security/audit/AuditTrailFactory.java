/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.audit;

import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.threadpool.ThreadPool;

@FunctionalInterface
public interface AuditTrailFactory {

    AuditTrail create(
        Settings settings,
        ClusterService clusterService,
        ThreadPool threadPool,
        SystemIndices systemIndices,
        ProjectResolver projectResolver
    );
}
