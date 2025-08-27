/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.queries;

import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.inference.InferenceResults;

import java.util.Collection;

public interface InferenceResultsProvider extends NamedWriteable {
    InferenceResults getInferenceResults(String inferenceId);

    Collection<InferenceResults> getAllInferenceResults();
}
