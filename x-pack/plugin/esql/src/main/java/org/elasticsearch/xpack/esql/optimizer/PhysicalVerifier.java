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
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.optimizer.rules.PlanConsistencyChecker;
import org.elasticsearch.xpack.esql.plan.logical.ExecutesOn;
import org.elasticsearch.xpack.esql.plan.physical.DocRefEncodeExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeExec;
import org.elasticsearch.xpack.esql.plan.physical.FetchExec;
import org.elasticsearch.xpack.esql.plan.physical.FetchSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.FieldExtractExec;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;

import java.util.List;

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

            if (p instanceof DocRefEncodeExec encode) {
                checkDocRefEncode(encode, failures);
            }
            if (p instanceof FetchExec fetch) {
                checkFetch(fetch, failures);
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
            checkExchangeScopes(optimizedPlan, false, failures, depFailures);
        }
    }

    /**
     * A {@link ExchangeExec.Scope#CLUSTER} exchange sends rows over the network, which a {@code _doc} column cannot cross.
     * A {@link ExchangeExec.Scope#NODE} exchange is split off on the data node without planning, so its fragment must be
     * correct as the coordinator wrote it: nested in a cluster exchange, declaring exactly the fragment's output and
     * internally consistent. Checking the fragment here turns a planner bug into a coordinator error instead of a data
     * node failure.
     */
    private static void checkExchangeScopes(PhysicalPlan plan, boolean belowClusterExchange, Failures failures, Failures depFailures) {
        boolean belowCluster = belowClusterExchange;
        if (plan instanceof ExchangeExec exchange) {
            switch (exchange.scope()) {
                case CLUSTER -> {
                    if (exchange.output().stream().anyMatch(a -> a.dataType() == DataType.DOC_DATA_TYPE)) {
                        failures.add(
                            fail(exchange, "document identity [_doc] cannot cross a cluster exchange [{}]", exchange.nodeString())
                        );
                    }
                    belowCluster = true;
                }
                case NODE -> checkNodeExchange(exchange, belowClusterExchange, failures, depFailures);
            }
        }
        for (PhysicalPlan child : plan.children()) {
            checkExchangeScopes(child, belowCluster, failures, depFailures);
        }
    }

    private static void checkNodeExchange(ExchangeExec exchange, boolean belowClusterExchange, Failures failures, Failures depFailures) {
        if (belowClusterExchange == false) {
            failures.add(fail(exchange, "a NODE exchange must be below a CLUSTER exchange [{}]", exchange.nodeString()));
        }
        if (exchange.child() instanceof FragmentExec fragment) {
            if (sameIdsAndTypes(exchange.output(), fragment.output()) == false) {
                failures.add(
                    fail(exchange, "NODE exchange output {} does not match its fragment output {}", exchange.output(), fragment.output())
                );
            }
            fragment.fragment().forEachDown(node -> PlanConsistencyChecker.checkPlan(node, depFailures));
        } else {
            failures.add(fail(exchange, "a NODE exchange must wrap a fragment, found [{}]", exchange.child().nodeName()));
        }
    }

    private static void checkDocRefEncode(DocRefEncodeExec encode, Failures failures) {
        if (encode.doc().dataType() != DataType.DOC_DATA_TYPE || encode.docRef().dataType() != DataType.DOC_REF) {
            failures.add(fail(encode, "[{}] must replace a [_doc] with a [DOC_REF] attribute", encode.nodeString()));
        }
    }

    /**
     * The fetch plan runs on the nodes that own the documents, exactly as the coordinator built it: it must produce the
     * fetched columns and nothing but loading may happen in it.
     */
    private static void checkFetch(FetchExec fetch, Failures failures) {
        if (fetch.docRef().dataType() != DataType.DOC_REF) {
            failures.add(fail(fetch, "[{}] must read a [DOC_REF] attribute", fetch.nodeString()));
        }
        if (fetch.stage() < 1) {
            failures.add(fail(fetch, "[{}] must have a stage of at least 1", fetch.nodeString()));
        }
        if (sameIdsAndTypes(fetch.fetchPlan().output(), fetch.fetchedAttributes()) == false) {
            failures.add(
                fail(
                    fetch,
                    "fetch plan output {} does not match the fetched attributes {}",
                    fetch.fetchPlan().output(),
                    fetch.fetchedAttributes()
                )
            );
        }
        fetch.fetchPlan().forEachDown(node -> {
            if (node instanceof ProjectExec == false
                && node instanceof FieldExtractExec == false
                && node instanceof FetchSourceExec == false) {
                failures.add(fail(node, "[{}] cannot run in a fetch plan", node.nodeName()));
            }
        });
    }

    private static boolean sameIdsAndTypes(List<Attribute> left, List<Attribute> right) {
        if (left.size() != right.size()) {
            return false;
        }
        for (int i = 0; i < left.size(); i++) {
            if (left.get(i).id().equals(right.get(i).id()) == false || left.get(i).dataType() != right.get(i).dataType()) {
                return false;
            }
        }
        return true;
    }
}
