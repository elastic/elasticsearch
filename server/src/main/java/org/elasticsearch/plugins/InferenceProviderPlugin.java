/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins;

import org.elasticsearch.inference.InferenceProvider;

/**
 * An extension point for {@link Plugin} implementations to add inference plugins for use on document ingestion
 */
public interface InferenceProviderPlugin {

    /**
     * Returns the inference provider added by this plugin.
     *
     * @return InferenceProvider added by the plugin
     */
    InferenceProvider getInferenceProvider();

}
