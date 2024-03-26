/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins;

import org.elasticsearch.inference.InferenceServiceRegistry;
import org.elasticsearch.inference.ModelRegistry;

/**
 * Plugins that provide inference services should implement this interface.
 * There should be a single one in the classpath, as we currently support a single instance for ModelRegistry / InfereceServiceRegistry.
 */
public interface InferenceRegistryPlugin {
    InferenceServiceRegistry getInferenceServiceRegistry();

    ModelRegistry getModelRegistry();
}
