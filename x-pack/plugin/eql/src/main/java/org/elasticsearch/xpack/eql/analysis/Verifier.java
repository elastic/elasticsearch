/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.analysis;

import org.elasticsearch.xpack.eql.plan.logical.Head;
import org.elasticsearch.xpack.eql.plan.logical.Join;
import org.elasticsearch.xpack.eql.plan.logical.KeyedFilter;
import org.elasticsearch.xpack.eql.plan.logical.Sequence;
import org.elasticsearch.xpack.eql.plan.logical.Tail;
import org.elasticsearch.xpack.eql.stats.FeatureMetric;
import org.elasticsearch.xpack.eql.stats.Metrics;
import org.elasticsearch.xpack.ql.capabilities.Unresolvable;
import org.elasticsearch.xpack.ql.common.Failure;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.NamedExpression;
import org.elasticsearch.xpack.ql.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.ql.util.StringUtils;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.xpack.eql.stats.FeatureMetric.EVENT;
import static org.elasticsearch.xpack.eql.stats.FeatureMetric.JOIN;
import static org.elasticsearch.xpack.eql.stats.FeatureMetric.JOIN_KEYS_FIVE_OR_MORE;
import static org.elasticsearch.xpack.eql.stats.FeatureMetric.JOIN_KEYS_FOUR;
import static org.elasticsearch.xpack.eql.stats.FeatureMetric.JOIN_KEYS_ONE;
import static org.elasticsearch.xpack.eql.stats.FeatureMetric.JOIN_KEYS_THREE;
import static org.elasticsearch.xpack.eql.stats.FeatureMetric.JOIN_KEYS_TWO;
import static org.elasticsearch.xpack.eql.stats.FeatureMetric.JOIN_QUERIES_FIVE_OR_MORE;
import static org.elasticsearch.xpack.eql.stats.FeatureMetric.JOIN_QUERIES_FOUR;
import static org.elasticsearch.xpack.eql.stats.FeatureMetric.JOIN_QUERIES_THREE;
import static org.elasticsearch.xpack.eql.stats.FeatureMetric.JOIN_QUERIES_TWO;
import static org.elasticsearch.xpack.eql.stats.FeatureMetric.JOIN_UNTIL;
import static org.elasticsearch.xpack.eql.stats.FeatureMetric.PIPE_HEAD;
import static org.elasticsearch.xpack.eql.stats.FeatureMetric.PIPE_TAIL;
import static org.elasticsearch.xpack.eql.stats.FeatureMetric.SEQUENCE;
import static org.elasticsearch.xpack.eql.stats.FeatureMetric.SEQUENCE_MAXSPAN;
import static org.elasticsearch.xpack.eql.stats.FeatureMetric.SEQUENCE_QUERIES_FIVE_OR_MORE;
import static org.elasticsearch.xpack.eql.stats.FeatureMetric.SEQUENCE_QUERIES_FOUR;
import static org.elasticsearch.xpack.eql.stats.FeatureMetric.SEQUENCE_QUERIES_THREE;
import static org.elasticsearch.xpack.eql.stats.FeatureMetric.SEQUENCE_QUERIES_TWO;
import static org.elasticsearch.xpack.eql.stats.FeatureMetric.SEQUENCE_UNTIL;
import static org.elasticsearch.xpack.ql.analyzer.VerifierChecks.checkFilterConditionType;
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

    Collection<Failure> verify(LogicalPlan plan) {
        Set<Failure> failures = new LinkedHashSet<>();

        // start bottom-up
        plan.forEachUp(p -> {
            if (p.getClass().equals(Join.class)) {
                failures.add(fail(p, "JOIN command is not supported"));
            }
            if (p.analyzed()) {
                return;
            }

            // if the children are unresolved, so will this node; counting it will only add noise
            if (p.childrenResolved() == false) {
                return;
            }

            Set<Failure> localFailures = new LinkedHashSet<>();

            if (p instanceof Unresolvable unresolvable) {
                localFailures.add(fail(p, unresolvable.unresolvedMessage()));
            } else {
                p.forEachExpression(e -> {
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
                            if (ae instanceof UnresolvedAttribute ua) {
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

        // Concrete verifications

        // if there are no (major) unresolved failures, do more in-depth analysis

        if (failures.isEmpty()) {
            Set<Failure> localFailures = new LinkedHashSet<>();

            plan.forEachDown(p -> {
                // if the children are unresolved, so will this node; counting it will only add noise
                if (p.childrenResolved() == false) {
                    return;
                }

                checkFilterConditionType(p, localFailures);
                checkJoinKeyTypes(p, localFailures);
                // mark the plan as analyzed
                // if everything checks out
                if (failures.isEmpty()) {
                    p.setAnalyzed();
                }
            });

            failures.addAll(localFailures);
        }

        // gather metrics
        if (failures.isEmpty()) {
            BitSet b = new BitSet(FeatureMetric.values().length);

            plan.forEachDown(p -> {
                if (p instanceof Head) {
                    b.set(PIPE_HEAD.ordinal());
                } else if (p instanceof Tail) {
                    b.set(PIPE_TAIL.ordinal());
                } else if (p instanceof Join j) {

                    if (p instanceof Sequence s) {
                        b.set(SEQUENCE.ordinal());
                        if (s.maxSpan().duration() > 0) {
                            b.set(SEQUENCE_MAXSPAN.ordinal());
                        }

                        int queriesCount = s.queries().size();
                        switch (queriesCount) {
                            case 2 -> b.set(SEQUENCE_QUERIES_TWO.ordinal());
                            case 3 -> b.set(SEQUENCE_QUERIES_THREE.ordinal());
                            case 4 -> b.set(SEQUENCE_QUERIES_FOUR.ordinal());
                            default -> b.set(SEQUENCE_QUERIES_FIVE_OR_MORE.ordinal());
                        }
                        if (j.until().keys().isEmpty() == false) {
                            b.set(SEQUENCE_UNTIL.ordinal());
                        }
                    } else {
                        b.set(FeatureMetric.JOIN.ordinal());
                        int queriesCount = j.queries().size();
                        switch (queriesCount) {
                            case 2 -> b.set(JOIN_QUERIES_TWO.ordinal());
                            case 3 -> b.set(JOIN_QUERIES_THREE.ordinal());
                            case 4 -> b.set(JOIN_QUERIES_FOUR.ordinal());
                            default -> b.set(JOIN_QUERIES_FIVE_OR_MORE.ordinal());
                        }
                        if (j.until().keys().isEmpty() == false) {
                            b.set(JOIN_UNTIL.ordinal());
                        }
                    }

                    int joinKeysCount = j.queries().get(0).keys().size();
                    switch (joinKeysCount) {
                        case 1:
                            b.set(JOIN_KEYS_ONE.ordinal());
                            break;
                        case 2:
                            b.set(JOIN_KEYS_TWO.ordinal());
                            break;
                        case 3:
                            b.set(JOIN_KEYS_THREE.ordinal());
                            break;
                        case 4:
                            b.set(JOIN_KEYS_FOUR.ordinal());
                            break;
                        default:
                            if (joinKeysCount >= 5) {
                                b.set(JOIN_KEYS_FIVE_OR_MORE.ordinal());
                            }
                            break;
                    }
                }
            });

            if (b.get(SEQUENCE.ordinal()) == false && b.get(JOIN.ordinal()) == false) {
                b.set(EVENT.ordinal());
            }

            for (int i = b.nextSetBit(0); i >= 0; i = b.nextSetBit(i + 1)) {
                metrics.inc(FeatureMetric.values()[i]);
            }
        }

        return failures;
    }

    private static void checkJoinKeyTypes(LogicalPlan plan, Set<Failure> localFailures) {
        if (plan instanceof Join join) {
            List<KeyedFilter> queries = join.queries();
            KeyedFilter until = join.until();
            // pick first query and iterate its keys
            KeyedFilter first = queries.get(0);
            List<? extends NamedExpression> keys = first.keys();
            for (int keyIndex = 0; keyIndex < keys.size(); keyIndex++) {
                NamedExpression currentKey = keys.get(keyIndex);
                for (int i = 1; i < queries.size(); i++) {
                    KeyedFilter filter = queries.get(i);
                    doCheckKeyTypes(join, localFailures, currentKey, filter.keys().get(keyIndex));
                    if (until.keys().isEmpty() == false) {
                        doCheckKeyTypes(join, localFailures, currentKey, until.keys().get(keyIndex));
                    }
                }
            }
        }
    }

    private static void doCheckKeyTypes(Join join, Set<Failure> localFailures, NamedExpression expectedKey, NamedExpression currentKey) {
        if (DataTypes.areCompatible(expectedKey.dataType(), currentKey.dataType()) == false) {
            localFailures.add(
                fail(
                    currentKey,
                    "{} key [{}] type [{}] is incompatible with key [{}] type [{}]",
                    join.nodeName(),
                    currentKey.name(),
                    currentKey.dataType().esType(),
                    expectedKey.name(),
                    expectedKey.dataType().esType()
                )
            );
        }
    }
}
