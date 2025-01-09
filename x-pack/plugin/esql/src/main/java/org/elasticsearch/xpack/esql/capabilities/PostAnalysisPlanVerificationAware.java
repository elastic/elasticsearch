/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.capabilities;

import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.function.BiConsumer;

public interface PostAnalysisPlanVerificationAware {

    BiConsumer<LogicalPlan, Failures> postAnalysisPlanVerification();
}
