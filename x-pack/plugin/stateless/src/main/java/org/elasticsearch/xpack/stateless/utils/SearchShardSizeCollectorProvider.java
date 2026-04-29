/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.utils;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.threadpool.ThreadPool;

/**
 * SPI for providing a search shard size collector.
 * If no implementation is provided, a no-op collector is used.
 */
public interface SearchShardSizeCollectorProvider {

    SearchShardSizeCollector create(ThreadPool threadPool, Client client, ClusterService clusterService);
}
