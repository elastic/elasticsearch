/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.expression.predicate.logical;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.xpack.esql.capabilities.PostAnalysisPlanVerificationAware;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.predicate.Negatable;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.fulltext.FullTextFunction;
import org.elasticsearch.xpack.esql.expression.predicate.Predicates;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.LucenePushdownPredicates;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.io.IOException;
import java.util.function.BiConsumer;

import static org.elasticsearch.xpack.esql.common.Failure.fail;

public class Or extends BinaryLogic implements Negatable<BinaryLogic>, PostAnalysisPlanVerificationAware {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Or", Or::new);

    public Or(Source source, Expression left, Expression right) {
        super(source, left, right, BinaryLogicOperation.OR);
    }

    private Or(StreamInput in) throws IOException {
        super(in, BinaryLogicOperation.OR);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<Or> info() {
        return NodeInfo.create(this, Or::new, left(), right());
    }

    @Override
    protected Or replaceChildren(Expression newLeft, Expression newRight) {
        return new Or(source(), newLeft, newRight);
    }

    @Override
    public Or swapLeftAndRight() {
        return new Or(source(), right(), left());
    }

    @Override
    public And negate() {
        return new And(source(), Not.negate(left()), Not.negate(right()));
    }

    @Override
    protected Expression canonicalize() {
        // NB: this add a circular dependency between Predicates / Logical package
        return Predicates.combineOr(Predicates.splitOr(super.canonicalize()));
    }

    @Override
    public boolean translatable(LucenePushdownPredicates pushdownPredicates) {
        return super.translatable(pushdownPredicates) && checkPushableFullTextSearchFunctions();
    }

    @Override
    public BiConsumer<LogicalPlan, Failures> postAnalysisPlanVerification() {
        return (plan, failures) -> {
            boolean usesScore = plan.output()
                .stream()
                .anyMatch(attr -> attr instanceof MetadataAttribute ma && ma.name().equals(MetadataAttribute.SCORE));
            if (usesScore && checkPushableFullTextSearchFunctions() == false) {
                failures.add(
                    fail(
                        this,
                        "Invalid condition when using METADATA _score [{}]. Full text functions can be used in an OR condition, "
                            + "but only if just full text functions are used in the OR condition",
                        sourceText()
                    )
                );
            }
        };
    }

    private boolean checkPushableFullTextSearchFunctions() {
        boolean hasFullText = anyMatch(FullTextFunction.class::isInstance);
        return hasFullText == false || onlyFullTextFunctionsInExpression(this);
    }

    /**
     * Checks whether an expression contains just full text functions or negations (NOT) and combinations (AND, OR) of full text functions
     *
     * @param expression expression to check
     * @return true if all children are full text functions or negations of full text functions, false otherwise
     */
    private static boolean onlyFullTextFunctionsInExpression(Expression expression) {
        if (expression instanceof FullTextFunction) {
            return true;
        } else if (expression instanceof Not) {
            return onlyFullTextFunctionsInExpression(expression.children().get(0));
        } else if (expression instanceof BinaryLogic binaryLogic) {
            return onlyFullTextFunctionsInExpression(binaryLogic.left()) && onlyFullTextFunctionsInExpression(binaryLogic.right());
        }

        return false;
    }
}
