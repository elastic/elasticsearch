/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.analysis;

import org.elasticsearch.xpack.eql.plan.logical.Head;
import org.elasticsearch.xpack.eql.plan.logical.Join;
import org.elasticsearch.xpack.eql.plan.logical.Sequence;
import org.elasticsearch.xpack.eql.plan.logical.Tail;
import org.elasticsearch.xpack.eql.stats.FeatureMetric;
import org.elasticsearch.xpack.eql.stats.Metrics;
import org.elasticsearch.xpack.ql.capabilities.Unresolvable;
import org.elasticsearch.xpack.ql.common.Failure;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.plan.logical.Project;
import org.elasticsearch.xpack.ql.tree.Node;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.ql.util.Holder;
import org.elasticsearch.xpack.ql.util.StringUtils;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.stream.Collectors.toMap;
import static org.elasticsearch.xpack.eql.stats.FeatureMetric.EVENT;
import static org.elasticsearch.xpack.eql.stats.FeatureMetric.JOIN;
import static org.elasticsearch.xpack.eql.stats.FeatureMetric.JOIN_UNTIL;
import static org.elasticsearch.xpack.eql.stats.FeatureMetric.JOIN_KEYS_FIVE_OR_MORE;
import static org.elasticsearch.xpack.eql.stats.FeatureMetric.JOIN_KEYS_FOUR;
import static org.elasticsearch.xpack.eql.stats.FeatureMetric.JOIN_KEYS_ONE;
import static org.elasticsearch.xpack.eql.stats.FeatureMetric.JOIN_KEYS_THREE;
import static org.elasticsearch.xpack.eql.stats.FeatureMetric.JOIN_KEYS_TWO;
import static org.elasticsearch.xpack.eql.stats.FeatureMetric.JOIN_QUERIES_FIVE_OR_MORE;
import static org.elasticsearch.xpack.eql.stats.FeatureMetric.JOIN_QUERIES_FOUR;
import static org.elasticsearch.xpack.eql.stats.FeatureMetric.JOIN_QUERIES_THREE;
import static org.elasticsearch.xpack.eql.stats.FeatureMetric.JOIN_QUERIES_TWO;
import static org.elasticsearch.xpack.eql.stats.FeatureMetric.PIPE_HEAD;
import static org.elasticsearch.xpack.eql.stats.FeatureMetric.PIPE_TAIL;
import static org.elasticsearch.xpack.eql.stats.FeatureMetric.SEQUENCE;
import static org.elasticsearch.xpack.eql.stats.FeatureMetric.SEQUENCE_UNTIL;
import static org.elasticsearch.xpack.eql.stats.FeatureMetric.SEQUENCE_MAXSPAN;
import static org.elasticsearch.xpack.eql.stats.FeatureMetric.SEQUENCE_QUERIES_FIVE_OR_MORE;
import static org.elasticsearch.xpack.eql.stats.FeatureMetric.SEQUENCE_QUERIES_FOUR;
import static org.elasticsearch.xpack.eql.stats.FeatureMetric.SEQUENCE_QUERIES_THREE;
import static org.elasticsearch.xpack.eql.stats.FeatureMetric.SEQUENCE_QUERIES_TWO;
import static org.elasticsearch.xpack.ql.common.Failure.fail;

/**
 * The verifier has the role of checking the analyzed tree for failures and build a list of failures following this check.
 * It is created in the plan executor along with the metrics instance passed as constructor parameter.
 */
public class Verifier {

    private final Metrics metrics;

    public Verifier(Metrics metrics) {
        this.metrics = metrics;
    }

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

        // gather metrics
        if (failures.isEmpty()) {
            BitSet b = new BitSet(FeatureMetric.values().length);
            Holder<Boolean> isLikelyAnEventQuery = new Holder<>(false);

            plan.forEachDown(p -> {
                if (p instanceof Project) {
                    isLikelyAnEventQuery.set(true);
                } else if (p instanceof Head) {
                    b.set(PIPE_HEAD.ordinal());
                } else if (p instanceof Tail) {
                    b.set(PIPE_TAIL.ordinal());
                } else if (p instanceof Join) {
                    Join j = (Join) p;

                    if (p instanceof Sequence) {
                        b.set(SEQUENCE.ordinal());
                        Sequence s = (Sequence) p;
                        if (s.maxSpan().duration() > 0) {
                            b.set(SEQUENCE_MAXSPAN.ordinal());
                        }
                        
                        int queriesCount = s.queries().size();
                        switch (queriesCount) {
                            case 2:  b.set(SEQUENCE_QUERIES_TWO.ordinal());
                                     break;
                            case 3:  b.set(SEQUENCE_QUERIES_THREE.ordinal());
                                     break;
                            case 4:  b.set(SEQUENCE_QUERIES_FOUR.ordinal());
                                     break;
                            default: b.set(SEQUENCE_QUERIES_FIVE_OR_MORE.ordinal());
                                     break;
                        }
                        if (j.until().keys().isEmpty() == false) {
                            b.set(SEQUENCE_UNTIL.ordinal());
                        }
                    } else {
                        b.set(FeatureMetric.JOIN.ordinal());
                        int queriesCount = j.queries().size();
                        switch (queriesCount) {
                            case 2:  b.set(JOIN_QUERIES_TWO.ordinal());
                                     break;
                            case 3:  b.set(JOIN_QUERIES_THREE.ordinal());
                                     break;
                            case 4:  b.set(JOIN_QUERIES_FOUR.ordinal());
                                     break;
                            default: b.set(JOIN_QUERIES_FIVE_OR_MORE.ordinal());
                                     break;
                        }
                        if (j.until().keys().isEmpty() == false) {
                            b.set(JOIN_UNTIL.ordinal());
                        }
                    }

                    int joinKeysCount = j.queries().get(0).keys().size();
                    switch (joinKeysCount) {
                        case 1:  b.set(JOIN_KEYS_ONE.ordinal());
                                 break;
                        case 2:  b.set(JOIN_KEYS_TWO.ordinal());
                                 break;
                        case 3:  b.set(JOIN_KEYS_THREE.ordinal());
                                 break;
                        case 4:  b.set(JOIN_KEYS_FOUR.ordinal());
                                 break;
                        default: if (joinKeysCount >= 5) {
                                     b.set(JOIN_KEYS_FIVE_OR_MORE.ordinal());
                                 }
                                 break;
                    }
                }
            });

            if (isLikelyAnEventQuery.get() && b.get(SEQUENCE.ordinal()) == false && b.get(JOIN.ordinal()) == false) {
                b.set(EVENT.ordinal());
            }

            for (int i = b.nextSetBit(0); i >= 0; i = b.nextSetBit(i + 1)) {
                metrics.inc(FeatureMetric.values()[i]);
            }
        }

        return failures;
    }
}
