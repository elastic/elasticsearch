/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.analysis.analyzer;

import org.elasticsearch.xpack.sql.expression.Attribute;
import org.elasticsearch.xpack.sql.expression.AttributeSet;
import org.elasticsearch.xpack.sql.expression.Expressions;
import org.elasticsearch.xpack.sql.expression.NamedExpression;
import org.elasticsearch.xpack.sql.expression.function.Functions;
import org.elasticsearch.xpack.sql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.sql.expression.function.scalar.Cast;
import org.elasticsearch.xpack.sql.plan.logical.Aggregate;
import org.elasticsearch.xpack.sql.plan.logical.Filter;
import org.elasticsearch.xpack.sql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.sql.plan.logical.Project;
import org.elasticsearch.xpack.sql.tree.Node;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;

import static java.lang.String.format;
import static java.util.stream.Collectors.toList; 

abstract class Verifier {

    static class Failure {
        private final Node<?> source;
        private final String message;
        
        Failure(Node<?> source, String message) {
            this.source = source;
            this.message = message;
        }

        Node<?> source() {
            return source;
        }

        String message() {
            return message;
        }

        @Override
        public int hashCode() {
            return source.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            
            Verifier.Failure other = (Verifier.Failure) obj;
            return Objects.equals(source, other.source);
        }

        @Override
        public String toString() {
            return message;
        }
    }

    private static Failure fail(Node<?> source, String message, Object... args) {
        return new Failure(source, format(Locale.ROOT, message, args));
    }

    static Collection<Failure> verify(LogicalPlan plan) {
        Set<Failure> failures = new LinkedHashSet<>();

        plan.forEachUp(p -> {

            if (p.analyzed()) {
                return;
            }
            
            Set<Failure> localFailures = new LinkedHashSet<>();
            
            //
            // Handle unresolved items first
            //
            
            // first look at expressions
            p.forEachExpressions(e -> e.forEachUp(ae -> {
                if (ae.typeResolved().unresolved()) {
                    localFailures.add(fail(ae, ae.typeResolved().message()));
                }
                else if (ae.childrenResolved() && !ae.resolved()) {
                    localFailures.add(fail(ae, "Unresolved item '%s'", Expressions.name(e)));
                }
                else if (ae instanceof Attribute && !ae.resolved()) {
                    localFailures.add(fail(e, "Cannot resolved '%s' from columns %s", Expressions.name(ae), p.intputSet()));
                }
                else if (ae instanceof Cast && !(p instanceof Project || p instanceof Aggregate)) {
                    localFailures.add(fail(ae, "Cast is (currently) only supported in SELECT and GROUP BY; not in %s", p.nodeName()));
                }
            }));

            // consider only nodes that are by themselves unresolved (to avoid unresolved dependees)
            if (p.childrenResolved() && p.expressionsResolved() && !p.resolved()) {
                localFailures.add(fail(p, "Unresolved item '%s'", p.nodeString()));
            }

            //
            // Handle incorrect statement
            // 

            havingContainsOnlyExistingAggs(p, localFailures);
            
            // everything checks out
            // mark the plan as analyzed
            if (localFailures.isEmpty()) {
                p.setAnalyzed();
            }

            failures.addAll(localFailures);
        });

        return failures;
    }

    private static void havingContainsOnlyExistingAggs(LogicalPlan p, Set<Failure> failures) {
        if (p instanceof Filter) {
            Filter f = (Filter) p;
            if (f.child() instanceof Aggregate) {
                Aggregate a = (Aggregate) f.child();
                
                List<Attribute> aggs = new ArrayList<>();
                
                a.aggregates().forEach(ne -> {
                    AggregateFunction af = Functions.extractAggregate(ne);
                    if (af != null) {
                        aggs.add(af.toAttribute());
                    }
                });
                
                
                final List<Attribute> filterAggs = new ArrayList<>();
                f.condition().forEachUp(fa -> filterAggs.add(fa.toAttribute()), AggregateFunction.class);
                
                AttributeSet missing = new AttributeSet(filterAggs).substract(new AttributeSet(aggs));
                if (!missing.isEmpty()) {
                    List<String> missingNames = missing.stream()
                        .map(NamedExpression::name)
                        .collect(toList());
                    
                    List<String> expectedNames = aggs.stream()
                            .map(NamedExpression::name)
                            .collect(toList());
                    
                    failures.add(fail(p, "HAVING contains aggregations %s, expected one of %s ", missingNames, expectedNames));
                }
            }
        }
    }
}