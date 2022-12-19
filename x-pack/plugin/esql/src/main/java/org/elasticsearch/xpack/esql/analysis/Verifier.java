/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.xpack.ql.capabilities.Unresolvable;
import org.elasticsearch.xpack.ql.common.Failure;
import org.elasticsearch.xpack.ql.expression.Alias;
import org.elasticsearch.xpack.ql.expression.FieldAttribute;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.ReferenceAttribute;
import org.elasticsearch.xpack.ql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.ql.plan.logical.Aggregate;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;

import static org.elasticsearch.xpack.ql.common.Failure.fail;

public class Verifier {
    Collection<Failure> verify(LogicalPlan plan) {
        Set<Failure> failures = new LinkedHashSet<>();

        plan.forEachUp(p -> {
            // if the children are unresolved, so will this node; counting it will only add noise
            if (p.childrenResolved() == false) {
                return;
            }

            if (p instanceof Unresolvable u) {
                failures.add(Failure.fail(p, u.unresolvedMessage()));
            }
            p.forEachExpression(e -> {
                if (e instanceof Unresolvable u) {
                    failures.add(Failure.fail(e, u.unresolvedMessage()));
                }
                if (e.typeResolved().unresolved()) {
                    failures.add(fail(e, e.typeResolved().message()));
                }
            });

            if (p instanceof Aggregate agg) {
                agg.aggregates().forEach(e -> {
                    var exp = e instanceof Alias ? ((Alias) e).child() : e;
                    if (exp instanceof AggregateFunction aggFunc) {
                        aggFunc.arguments().forEach(a -> {
                            // TODO: allow an expression?
                            if ((a instanceof FieldAttribute || a instanceof ReferenceAttribute || a instanceof Literal) == false) {
                                failures.add(
                                    fail(
                                        e,
                                        "aggregate function's parameters must be an attribute or literal; found ["
                                            + a.sourceText()
                                            + "] of type ["
                                            + a.nodeName()
                                            + "]"
                                    )
                                );
                            }
                        });
                    } else if (agg.groupings().contains(exp) == false) { // TODO: allow an expression?
                        failures.add(
                            fail(
                                exp,
                                "expected an aggregate function or group but got ["
                                    + exp.sourceText()
                                    + "] of type ["
                                    + exp.nodeName()
                                    + "]"
                            )
                        );
                    }
                });
            }
        });

        return failures;
    }
}
