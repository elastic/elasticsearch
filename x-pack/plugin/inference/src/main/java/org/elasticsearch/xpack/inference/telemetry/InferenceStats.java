/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.telemetry;

import org.elasticsearch.inference.Model;

public interface InferenceStats {

    /**
     * Increment the counter for a particular value in a thread safe manner.
     * @param model the model to increment request count for
     */
    void incrementRequestCount(Model model);

    InferenceStats NOOP = model -> {};
}
