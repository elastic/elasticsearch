/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.analysis;

import org.elasticsearch.xpack.eql.common.Failure;
import org.elasticsearch.xpack.ql.capabilities.Unresolvable;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.tree.Node;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.ql.util.StringUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.stream.Collectors.toMap;
import static org.elasticsearch.xpack.eql.common.Failure.fail;

/**
 * The verifier has the role of checking the analyzed tree for failures and build a list of failures following this check.
 * It is created in the plan executor along with the metrics instance passed as constructor parameter.
 */
public class Verifier {

    public Map<Node<?>, String> verifyFailures(LogicalPlan plan) {
        Collection<Failure> failures = verify(plan);
        return failures.stream().collect(toMap(Failure::node, Failure::message));
    }

    Collection<Failure> verify(LogicalPlan plan) {
        Set<Failure> failures = new LinkedHashSet<>();

        // start bottom-up
        plan.forEachUp(p -> {
            if (p.analyzed()) {
                return;
            }

            // if the children are unresolved, so will this node; counting it will only add noise
            if (p.childrenResolved() == false) {
                return;
            }

            Set<Failure> localFailures = new LinkedHashSet<>();

            if (p instanceof Unresolvable) {
                localFailures.add(fail(p, ((Unresolvable) p).unresolvedMessage()));
            } else {
                p.forEachExpressions(e -> {
                    // everything is fine, skip expression
                    if (e.resolved()) {
                        return;
                    }

                    e.forEachUp(ae -> {
                        // we're only interested in the children
                        if (ae.childrenResolved() == false) {
                            return;
                        }
                        if (ae instanceof Unresolvable) {
                            // handle Attributes differently to provide more context
                            if (ae instanceof UnresolvedAttribute) {
                                UnresolvedAttribute ua = (UnresolvedAttribute) ae;
                                // only work out the synonyms for raw unresolved attributes
                                if (ua.customMessage() == false) {
                                    boolean useQualifier = ua.qualifier() != null;
                                    List<String> potentialMatches = new ArrayList<>();
                                    for (Attribute a : p.inputSet()) {
                                        String nameCandidate = useQualifier ? a.qualifiedName() : a.name();
                                        // add only primitives (object types would only result in another error)
                                        if (DataTypes.isUnsupported(a.dataType()) == false && DataTypes.isPrimitive(a.dataType())) {
                                            potentialMatches.add(nameCandidate);
                                        }
                                    }

                                    List<String> matches = StringUtils.findSimilar(ua.qualifiedName(), potentialMatches);
                                    if (matches.isEmpty() == false) {
                                        ae = ua.withUnresolvedMessage(UnresolvedAttribute.errorMessage(ua.qualifiedName(), matches));
                                    }
                                }
                            }

                            localFailures.add(fail(ae, ((Unresolvable) ae).unresolvedMessage()));
                            return;
                        }
                        // type resolution
                        if (ae.typeResolved().unresolved()) {
                            localFailures.add(fail(ae, ae.typeResolved().message()));
                        }

                    });
                });
            }

            failures.addAll(localFailures);
        });

        return failures;
    }
}
