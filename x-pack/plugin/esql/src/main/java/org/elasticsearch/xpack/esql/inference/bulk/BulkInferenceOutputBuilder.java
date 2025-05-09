/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference.bulk;

import org.elasticsearch.core.Releasable;
import org.elasticsearch.inference.InferenceServiceResults;

public interface BulkInferenceOutputBuilder<IR extends InferenceServiceResults, OutputType> extends Releasable {
    void addInferenceResults(IR inferenceResults);

    Class<IR> inferenceResultsClass();

    OutputType buildOutput();
}
