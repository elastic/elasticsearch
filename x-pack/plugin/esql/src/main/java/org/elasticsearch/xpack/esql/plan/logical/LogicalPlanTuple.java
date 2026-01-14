/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

/**
 * @param stubReplacedSubPlan - the completed / "destubbed" right-hand side of the bottommost InlineJoin in the plan. For example:
 *                            Aggregate[[],[MAX(x{r}#99,true[BOOLEAN]) AS y#102]]
 *                            \_Limit[1000[INTEGER],false]
 *                              \_LocalRelation[[x{r}#99],[IntVectorBlock[vector=ConstantIntVector[positions=1, value=1]]]]
 * @param originalSubPlan - the original (unchanged) right-hand side of the bottommost InlineJoin in the plan. For example:
 *                        Aggregate[[],[MAX(x{r}#99,true[BOOLEAN]) AS y#102]]
 *                        \_StubRelation[[x{r}#99]]]
 */
public record LogicalPlanTuple(LogicalPlan stubReplacedSubPlan, LogicalPlan originalSubPlan) {}
