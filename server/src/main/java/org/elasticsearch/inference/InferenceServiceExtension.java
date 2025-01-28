/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.List;

/**
 * SPI extension that define inference services
 */
public interface InferenceServiceExtension {

    List<Factory> getInferenceServiceFactories();

    record InferenceServiceFactoryContext(Client client, ThreadPool threadPool, ClusterService clusterService, Settings settings) {}

    interface Factory {
        /**
         * InferenceServices are created from the factory context
         */
        InferenceService create(InferenceServiceFactoryContext context);
    }
}
