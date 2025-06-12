/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.xpack.esql.core.expression.Expression;

/**
 * Interface signaling to the planner that an aggregation function has to be
 * corrected in the presence of random sampling.
 */
public interface HasSampleCorrection {

    boolean isSampleCorrected();

    Expression sampleCorrection(Expression sampleProbability);
}
