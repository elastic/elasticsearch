/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.scalar.ip.CIDRMatch;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.esql.expression.predicate.Predicates.combineOr;
import static org.elasticsearch.xpack.esql.expression.predicate.Predicates.splitOr;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.ipToString;

/**
 * Combine disjunctive Equals, In or CIDRMatch expressions on the same field into an In or CIDRMatch expression.
 * This rule looks for both simple equalities:
 * 1. a == 1 OR a == 2 becomes a IN (1, 2)
 * and combinations of In
 * 2. a == 1 OR a IN (2) becomes a IN (1, 2)
 * 3. a IN (1) OR a IN (2) becomes a IN (1, 2)
 * and combinations of CIDRMatch
 * 4. CIDRMatch(a, ip1) OR CIDRMatch(a, ip2) OR a == ip3 or a IN (ip4, ip5) becomes CIDRMatch(a, ip1, ip2, ip3, ip4, ip5)
 * <p>
 * This rule does NOT check for type compatibility as that phase has been
 * already be verified in the analyzer.
 */
public final class CombineDisjunctions extends OptimizerRules.OptimizerExpressionRule<Or> {
    public CombineDisjunctions() {
        super(OptimizerRules.TransformDirection.UP);
    }

    protected static In createIn(Expression key, List<Expression> values, ZoneId zoneId) {
        return new In(key.source(), key, values);
    }

    protected static Equals createEquals(Expression k, Set<Expression> v, ZoneId finalZoneId) {
        return new Equals(k.source(), k, v.iterator().next(), finalZoneId);
    }

    protected static CIDRMatch createCIDRMatch(Expression k, List<Expression> v) {
        return new CIDRMatch(k.source(), k, v);
    }

    @Override
    public Expression rule(Or or, LogicalOptimizerContext ctx) {
        Expression e = or;
        // look only at equals, In and CIDRMatch
        List<Expression> exps = splitOr(e);

        Map<Expression, Set<Expression>> ins = new LinkedHashMap<>();
        Map<Expression, Set<Expression>> cidrs = new LinkedHashMap<>();
        Map<Expression, Set<Expression>> ips = new LinkedHashMap<>();
        ZoneId zoneId = null;
        List<Expression> ors = new LinkedList<>();
        boolean changed = false;
        for (Expression exp : exps) {
            if (exp instanceof Equals eq) {
                // consider only equals against foldables
                if (eq.right().foldable()) {
                    ins.computeIfAbsent(eq.left(), k -> new LinkedHashSet<>()).add(eq.right());
                    if (eq.left().dataType() == DataType.IP) {
                        Object value = eq.right().fold(ctx.foldCtx());
                        // ImplicitCasting and ConstantFolding(includes explicit casting) are applied before CombineDisjunctions.
                        // They fold the input IP string to an internal IP format. These happen to Equals and IN, but not for CIDRMatch,
                        // as CIDRMatch takes strings as input, ImplicitCasting does not apply to it, and the first input to CIDRMatch is a
                        // field, ConstantFolding does not apply to it either.
                        // If the data type is IP, convert the internal IP format in Equals and IN to the format that is compatible with
                        // CIDRMatch, and store them in a separate map, so that they can be combined into existing CIDRMatch later.
                        if (value instanceof BytesRef bytesRef) {
                            value = ipToString(bytesRef);
                        }
                        ips.computeIfAbsent(eq.left(), k -> new LinkedHashSet<>()).add(new Literal(Source.EMPTY, value, DataType.IP));
                    }
                } else {
                    ors.add(exp);
                }
                if (zoneId == null) {
                    zoneId = eq.zoneId();
                }
            } else if (exp instanceof In in) {
                ins.computeIfAbsent(in.value(), k -> new LinkedHashSet<>()).addAll(in.list());
                if (in.value().dataType() == DataType.IP) {
                    List<Expression> values = new ArrayList<>(in.list().size());
                    for (Expression i : in.list()) {
                        Object value = i.fold(ctx.foldCtx());
                        // Same as Equals.
                        if (value instanceof BytesRef bytesRef) {
                            value = ipToString(bytesRef);
                        }
                        values.add(new Literal(Source.EMPTY, value, DataType.IP));
                    }
                    ips.computeIfAbsent(in.value(), k -> new LinkedHashSet<>()).addAll(values);
                }
            } else if (exp instanceof CIDRMatch cm) {
                cidrs.computeIfAbsent(cm.ipField(), k -> new LinkedHashSet<>()).addAll(cm.matches());
            } else {
                ors.add(exp);
            }
        }

        if (cidrs.isEmpty() == false) {
            for (Expression f : ips.keySet()) {
                cidrs.computeIfAbsent(f, k -> new LinkedHashSet<>()).addAll(ips.get(f));
                ins.remove(f);
            }
        }

        if (ins.isEmpty() == false) {
            // combine equals alongside the existing ors
            final ZoneId finalZoneId = zoneId;
            ins.forEach(
                (k, v) -> { ors.add(v.size() == 1 ? createEquals(k, v, finalZoneId) : createIn(k, new ArrayList<>(v), finalZoneId)); }
            );

            changed = true;
        }

        if (cidrs.isEmpty() == false) {
            cidrs.forEach((k, v) -> { ors.add(createCIDRMatch(k, new ArrayList<>(v))); });
            changed = true;
        }

        if (changed) {
            // TODO: this makes a QL `or`, not an ESQL `or`
            Expression combineOr = combineOr(ors);
            // check the result semantically since the result might different in order
            // but be actually the same which can trigger a loop
            // e.g. a == 1 OR a == 2 OR null --> null OR a in (1,2) --> literalsOnTheRight --> cycle
            if (e.semanticEquals(combineOr) == false) {
                e = combineOr;
            }
        }

        return e;
    }
}
