/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.xpack.esql.plan.physical.ExchangeSinkExec;

/**
 * @param nodeReducePlan The plan to be executed on the node_reduce driver. This should <i>not</i> contain a
 * {@link org.elasticsearch.xpack.esql.plan.physical.FragmentExec}, but be a plan "sandwiched" between an {@link ExchangeSinkExec} and an
 * {@link org.elasticsearch.xpack.esql.plan.physical.ExchangeSourceExec}.
 * @param dataNodePlan The plan to be executed on the data driver. This may contain a
 * {@link org.elasticsearch.xpack.esql.plan.physical.FragmentExec}.
 */
record ReductionPlan(ExchangeSinkExec nodeReducePlan, ExchangeSinkExec dataNodePlan) {}
