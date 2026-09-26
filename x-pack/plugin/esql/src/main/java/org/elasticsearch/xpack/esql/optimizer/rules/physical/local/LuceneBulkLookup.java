/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.enrich.MatchConfig;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.EsqlBinaryComparison;
import org.elasticsearch.xpack.esql.optimizer.LookupPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerRules;
import org.elasticsearch.xpack.esql.plan.physical.BulkLookupMvFilterExec;
import org.elasticsearch.xpack.esql.plan.physical.ParameterizedQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;

import java.util.List;

import static org.elasticsearch.xpack.esql.optimizer.rules.logical.OptimizerRules.TransformDirection.UP;

/**
 * Checks {@link ParameterizedQueryExec} nodes to see if the conditions for the bulk lookup optimization are met.
 * Sets {@code bulkLookupLeft} and {@code bulkLookupRight} when they are.
 */
public class LuceneBulkLookup extends PhysicalOptimizerRules.ParameterizedOptimizerRule<PhysicalPlan, LookupPhysicalOptimizerContext> {

    private static final Logger logger = LogManager.getLogger(LuceneBulkLookup.class);

    public LuceneBulkLookup() {
        super(UP);
    }

    @Override
    protected PhysicalPlan rule(PhysicalPlan plan, LookupPhysicalOptimizerContext context) {
        if (plan instanceof ParameterizedQueryExec pqExec) {
            return applyBulkLookup(pqExec, context);
        }
        return plan;
    }

    private static PhysicalPlan applyBulkLookup(ParameterizedQueryExec plan, LookupPhysicalOptimizerContext context) {

        // optimization already applied?
        if (plan.bulkLookupLeft() != null && plan.bulkLookupRight() != null) {
            return plan;
        }

        // This optimization avoids Lucene queries so we can't use it when an AliasFilter is in effect.
        if (context.aliasFilter() != null && context.aliasFilter() != AliasFilter.EMPTY) {
            logger.debug("Cannot use bulk lookup on KEYWORD with aliasFilter {}", context.aliasFilter());
            return plan;
        }

        // Only use optimization for LOOKUP JOIN on single keyword and no other Lucene queries
        if (plan.query() != null) {
            logger.debug("Cannot use bulk lookup on KEYWORD with Lucene query {}", plan.query());
            return plan;
        }

        Expression expr = plan.joinOnConditions();

        // LOOKUP JOIN ON field

        List<MatchConfig> matchFields = plan.matchFields();
        if (expr == null && matchFields != null && matchFields.size() == 1) {
            MatchConfig matchField = plan.matchFields().get(0);
            if (matchField.type() == DataType.KEYWORD) {

                // Find rightAttribute in output.
                // Assumes rule runs before ReplaceSourceAttributes.
                //
                String fieldName = matchField.fieldName();
                Attribute rightAttribute = null;
                for (Attribute attr : plan.output()) {
                    if (attr.name().equals(fieldName)) {
                        rightAttribute = attr;
                        break;
                    }
                }
                if (rightAttribute != null) {
                    ReferenceAttribute leftAttribute = new ReferenceAttribute(plan.source(), null, fieldName, DataType.KEYWORD);

                    logger.debug("Bulk lookup on KEYWORD field {}", fieldName);
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
                } else {
                    logger.debug("Cannot use bulk lookup, could not find rightAttribute {}", fieldName);
                }
            } else {
                logger.debug("Cannot use bulk lookup on field type {}", matchField.type());
            }
        }

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

        logger.debug("Cannot use bulk lookup optimization");
        return plan;
    }
}
