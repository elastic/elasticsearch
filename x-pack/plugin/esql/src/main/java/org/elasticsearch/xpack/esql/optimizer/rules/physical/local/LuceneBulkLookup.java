/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.EsqlBinaryComparison;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerRules;
import org.elasticsearch.xpack.esql.plan.physical.BulkLookupMvFilterExec;
import org.elasticsearch.xpack.esql.plan.physical.LeafExec;
import org.elasticsearch.xpack.esql.plan.physical.ParameterizedQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;

import static org.elasticsearch.xpack.esql.optimizer.rules.logical.OptimizerRules.TransformDirection.UP;

/**
 * Checks {@link ParameterizedQueryExec} nodes to see if the conditions for the bulk lookup optimization are met.
 * Sets the useBulkLookup flag when they are.
 */
public class LuceneBulkLookup extends PhysicalOptimizerRules.OptimizerRule<LeafExec> {

    private static final Logger logger = LogManager.getLogger(LuceneBulkLookup.class);

    public LuceneBulkLookup() {
        super(UP);
    }

    @Override
    protected PhysicalPlan rule(LeafExec plan) {
        if (plan instanceof ParameterizedQueryExec pqExec) {
            return applyBulkLookup(pqExec);
        }
        return plan;
    }

    private static PhysicalPlan applyBulkLookup(ParameterizedQueryExec plan) {

        // optimization already applied?
        if (plan.bulkLookupLeft() != null && plan.bulkLookupRight() != null) {
            return plan;
        }

        // only use optimization for LOOKUP JOIN on single keyword
        if (plan.query() != null) {
            return plan;
        }

        Expression expr = plan.joinOnConditions();

        /*
        // LOOKUP JOIN ON field

        List<MatchConfig> matchFields = plan.matchFields();
        if (expr == null && matchFields != null && matchFields.size() == 1) {
            MatchConfig matchField = plan.matchFields().get(0);
            if (matchField.type() == DataType.KEYWORD) {
                logger.debug("Bulk lookup on KEYWORD field {}", matchField.fieldName());
                return new BulkLookupMvFilterExec(
                    plan.source(),
                    new ParameterizedQueryExec(
                        plan.source(),
                        plan.output(),
                        plan.matchFields(),
                        plan.joinOnConditions(),
                        plan.query(),
                        plan.emptyResult(),
                        matchField.fieldName()
                    ),
                    matchField.fieldName()
                );
            }
        }
        */

        // LOOKUP JOIN ON leftKeyword == rightKeyword

        if (expr instanceof EsqlBinaryComparison binaryComparison
            && binaryComparison.left() instanceof Attribute leftAttribute
            && binaryComparison.right() instanceof Attribute rightAttribute
            && binaryComparison.getFunctionType() == EsqlBinaryComparison.BinaryComparisonOperation.EQ
            && leftAttribute.dataType() == DataType.KEYWORD
            && rightAttribute.dataType() == DataType.KEYWORD) {
            logger.debug("Bulk lookup on KEYWORD expression {} == {}", leftAttribute.name(), rightAttribute.name());

            return new BulkLookupMvFilterExec(
                plan.source(),
                new ParameterizedQueryExec(
                    plan.source(),
                    plan.output(),
                    plan.matchFields(),
                    plan.joinOnConditions(),
                    plan.query(),
                    plan.emptyResult(),
                    leftAttribute,
                    rightAttribute
                ),
                rightAttribute
            );
        }

        return plan;
    }
}
