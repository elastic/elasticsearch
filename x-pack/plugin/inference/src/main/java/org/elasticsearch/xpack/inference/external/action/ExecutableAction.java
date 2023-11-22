/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.inference.InferenceResults;

import java.util.List;

/**
 * Defines an inference request to a 3rd party service. The success or failure response is communicated through the provided listener.
 */
public interface ExecutableAction {
    void execute(List<String> input, ActionListener<List<? extends InferenceResults>> listener);
}
