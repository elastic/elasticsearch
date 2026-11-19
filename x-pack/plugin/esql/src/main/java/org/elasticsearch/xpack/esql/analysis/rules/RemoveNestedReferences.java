/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis.rules;

import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.common.Failure;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.expression.function.scalar.nested.NestedObject;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.rule.Rule;

import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

/**
 * Removes the {@code NESTED_OBJECT} reference columns once the nested functions have resolved their names through them. A
 * reference is not a value, so a plan that still reads one after that, such as a {@code WHERE}, a {@code STATS} or a
 * {@code FORK} branch, is rejected.
 */
public final class RemoveNestedReferences extends Rule<LogicalPlan, LogicalPlan> {
    @Override
    public LogicalPlan apply(LogicalPlan plan) {
        if (plan.anyMatch(p -> p.expressions().stream().anyMatch(e -> e.anyMatch(NestedObject.class::isInstance))) == false) {
            return plan;
        }
        Map<NameId, String> references = NestedReference.references(plan);
        Predicate<Expression> reference = e -> e instanceof NamedExpression named && references.containsKey(named.id());
        LogicalPlan cleaned = plan.transformUp(p -> remove(p, reference));
        cleaned.forEachDown(p -> {
            p.forEachExpression(Attribute.class, a -> checkNotAReference(a, references));
            if (p.resolved()) {
                p.output().forEach(a -> checkNotAReference(a, references));
            }
        });
        return cleaned;
    }

    private static LogicalPlan remove(LogicalPlan plan, Predicate<Expression> reference) {
        if (plan instanceof Project project) {
            List<? extends NamedExpression> projections = project.projections().stream().filter(p -> reference.test(p) == false).toList();
            return projections.size() == project.projections().size() ? project : project.withProjections(projections);
        }
        if (plan instanceof Eval eval) {
            var fields = eval.fields().stream().filter(f -> reference.test(f) == false).toList();
            if (fields.size() != eval.fields().size()) {
                return fields.isEmpty() ? eval.child() : new Eval(eval.source(), eval.child(), fields);
            }
        }
        return plan;
    }

    private static void checkNotAReference(Attribute attribute, Map<NameId, String> references) {
        String path = references.get(attribute.id());
        if (path != null) {
            throw new VerificationException(
                List.of(
                    Failure.fail(
                        attribute,
                        "[" + attribute.name() + "] is a reference to the nested collection [" + path + "]; read it with NESTED_EXPAND"
                    )
                )
            );
        }
    }
}
