/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.xpack.esql.capabilities.PostPhysicalOptimizationVerificationAware;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.optimizer.rules.PlanConsistencyChecker;
import org.elasticsearch.xpack.esql.plan.logical.ExecutesOn;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeExec;
import org.elasticsearch.xpack.esql.plan.physical.FetchExec;
import org.elasticsearch.xpack.esql.plan.physical.FieldExtractExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;

import java.util.HashSet;
import java.util.Set;

import static org.elasticsearch.xpack.esql.common.Failure.fail;

/** Physical plan verifier. */
public final class PhysicalVerifier extends PostOptimizationPhasePlanVerifier<PhysicalPlan> {
    public static final PhysicalVerifier LOCAL_INSTANCE = new PhysicalVerifier(true);
    public static final PhysicalVerifier INSTANCE = new PhysicalVerifier(false);

    private PhysicalVerifier(boolean isLocal) {
        super(isLocal);
    }

    @Override
    protected void checkPlanConsistency(PhysicalPlan optimizedPlan, Failures failures, Failures depFailures) {
        optimizedPlan.forEachDown(p -> {
            if (p instanceof FieldExtractExec fieldExtractExec) {
                Attribute sourceAttribute = fieldExtractExec.sourceAttribute();
                if (sourceAttribute == null) {
                    failures.add(
                        fail(
                            fieldExtractExec,
                            "Need to add field extractor for [{}] but cannot detect source attributes from node [{}]",
                            Expressions.names(fieldExtractExec.attributesToExtract()),
                            fieldExtractExec.child()
                        )
                    );
                }
            }

            // This check applies only for coordinator physical plans (isLocal == false)
            if (isLocal == false && p instanceof ExecutesOn ex && ex.executesOn() == ExecutesOn.ExecuteLocation.REMOTE) {
                failures.add(
                    fail(
                        p,
                        "Physical plan contains remote executing operation [{}] in local part. "
                            + "This usually means this command is incompatible with some of the preceding commands.",
                        p.nodeName()
                    )
                );
            }

            if (p instanceof FetchExec fetch && EsQueryExec.isDocAttribute(fetch.doc()) == false) {
                failures.add(fail(fetch, "[{}] must read a [_doc] attribute", fetch.nodeString()));
            }

            PlanConsistencyChecker.checkPlan(p, depFailures);

            if (failures.hasFailures() == false) {
                if (p instanceof PostPhysicalOptimizationVerificationAware va) {
                    va.postPhysicalOptimizationVerification(failures);
                }
                p.forEachExpression(ex -> {
                    if (ex instanceof PostPhysicalOptimizationVerificationAware va) {
                        va.postPhysicalOptimizationVerification(failures);
                    }
                });
            }
        });

        if (isLocal == false) {
            checkDocumentsLeaveTheirNodeOnlyForAFetch(optimizedPlan, failures);
            if (optimizedPlan.output().stream().anyMatch(EsQueryExec::isDocAttribute)) {
                failures.add(fail(optimizedPlan, "document identity [_doc] cannot be returned, found {}", optimizedPlan.output()));
            }
        }
    }

    /**
     * Rows carry their {@code _doc} across an exchange only for a {@link FetchExec} above it, which loads the documents
     * from the nodes that own them. The data nodes then keep the readers of those documents open and detach them, so the
     * documents still name their readers on the coordinator. Without the fetch, nothing would do either.
     */
    private static void checkDocumentsLeaveTheirNodeOnlyForAFetch(PhysicalPlan plan, Failures failures) {
        Set<NameId> fetched = new HashSet<>();
        plan.forEachDown(FetchExec.class, fetch -> fetched.add(fetch.doc().id()));
        plan.forEachDown(ExchangeExec.class, exchange -> {
            for (Attribute attribute : exchange.output()) {
                if (EsQueryExec.isDocAttribute(attribute) && fetched.contains(attribute.id()) == false) {
                    failures.add(
                        fail(exchange, "document identity [_doc] can only cross an exchange for a fetch [{}]", exchange.nodeString())
                    );
                }
            }
        });
    }
}
