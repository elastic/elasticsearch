/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules;

import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.plan.QueryPlan;
import org.elasticsearch.xpack.esql.plan.logical.BinaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.Fork;
import org.elasticsearch.xpack.esql.plan.physical.BinaryExec;
import org.elasticsearch.xpack.esql.plan.physical.MergeExec;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.esql.common.Failure.fail;

public class PlanConsistencyChecker {

    /**
     * Check whether a single {@link QueryPlan} produces no duplicate attributes and its children provide all of its required
     * {@link QueryPlan#references() references}. Otherwise, add
     * {@link org.elasticsearch.xpack.esql.common.Failure Failure}s to the {@link Failures} object.
     */
    public static void checkPlan(QueryPlan<?> p, Failures failures) {
        if (p instanceof BinaryPlan binaryPlan) {
            checkMissingBinary(
                p,
                binaryPlan.leftReferences(),
                binaryPlan.left().outputSet(),
                binaryPlan.rightReferences(),
                binaryPlan.right().outputSet(),
                failures
            );
        } else if (p instanceof BinaryExec binaryExec) {
            checkMissingBinary(
                p,
                binaryExec.leftReferences(),
                binaryExec.left().outputSet(),
                binaryExec.rightReferences(),
                binaryExec.right().outputSet(),
                failures
            );
        } else if (p instanceof Fork || p instanceof MergeExec) {
            checkMissingFork(p, failures);
        } else {
            checkMissing(p, p.references(), p.inputSet(), "missing references", failures);
        }

        var outputAttributeNames = Sets.<String>newHashSetWithExpectedSize(p.output().size());
        var outputAttributeIds = Sets.<NameId>newHashSetWithExpectedSize(p.output().size());
        for (Attribute outputAttr : p.output()) {
            if (outputAttributeNames.add(outputAttr.name()) == false || outputAttributeIds.add(outputAttr.id()) == false) {
                failures.add(
                    fail(p, "Plan [{}] optimized incorrectly due to duplicate output attribute {}", p.nodeString(), outputAttr.toString())
                );
            }
        }
    }

    private static void checkMissingFork(QueryPlan<?> plan, Failures failures) {
        for (QueryPlan<?> child : plan.children()) {
            checkMissingForkBranch(child, plan.outputSet(), failures);
        }
    }

    private static void checkMissingForkBranch(QueryPlan<?> plan, AttributeSet forkOutputSet, Failures failures) {
        Map<String, DataType> attributeTypes = forkOutputSet.stream().collect(Collectors.toMap(Attribute::name, Attribute::dataType));
        AttributeSet missing = AttributeSet.of();

        Set<String> commonAttrs = new HashSet<>();

        // get the missing attributes from the sub plan
        plan.output().forEach(attribute -> {
            var attrType = attributeTypes.get(attribute.name());
            if (attrType == null || attrType != attribute.dataType()) {
                missing.add(attribute);
            }
            commonAttrs.add(attribute.name());
        });

        // get the missing attributes from the fork output
        forkOutputSet.forEach(attribute -> {
            if (commonAttrs.contains(attribute.name()) == false) {
                missing.add(attribute);
            }
        });

        if (missing.isEmpty() == false) {
            failures.add(
                fail(plan, "Plan [{}] optimized incorrectly due to missing attributes in subplans", plan.nodeString(), missing.toString())
            );
        }
    }

    private static void checkMissingBinary(
        QueryPlan<?> plan,
        AttributeSet leftReferences,
        AttributeSet leftInput,
        AttributeSet rightReferences,
        AttributeSet rightInput,
        Failures failures
    ) {
        checkMissing(plan, leftReferences, leftInput, "missing references from left hand side", failures);
        checkMissing(plan, rightReferences, rightInput, "missing references from right hand side", failures);
    }

    private static void checkMissing(
        QueryPlan<?> plan,
        AttributeSet references,
        AttributeSet input,
        String detailErrorMessage,
        Failures failures
    ) {
        AttributeSet missing = references.subtract(input);
        if (missing.isEmpty() == false) {
            failures.add(fail(plan, "Plan [{}] optimized incorrectly due to {} {}", plan.nodeString(), detailErrorMessage, missing));
        }
    }
}
