/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.inference.InferenceService;

import java.util.List;

/**
 * InferenceServicePlugins implement an inference service
 */
public interface InferenceServicePlugin {

    List<Factory> getInferenceServiceFactories();

    record InferenceServiceFactoryContext(Client client) {}

    interface Factory {
        /**
         * InferenceServices are created from the factory context
         */
        InferenceService create(InferenceServiceFactoryContext context);
    }

    /**
     * The named writables defined and used by each of the implemented
     * InferenceServices. Each service should define named writables for
     * - {@link org.elasticsearch.inference.TaskSettings}
     * - {@link org.elasticsearch.inference.ServiceSettings}
     * And optionally for {@link org.elasticsearch.inference.InferenceResults}
     * if the service uses a new type of result.
     * @return All named writables defined by the services
     */
    List<NamedWriteableRegistry.Entry> getInferenceServiceNamedWriteables();
}
