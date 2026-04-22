/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.engine;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;

/**
 * Factory for creating {@link RefreshManagerService} instances.
 * The exact implementation is provided via SPI - otherwise, a default no-op implementation is used.
 */
public interface RefreshManagerServiceFactory {

    RefreshManagerService create(Settings settings, ClusterService clusterService);
}
