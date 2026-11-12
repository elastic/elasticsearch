/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.xpack.esql.capabilities.PostPhysicalOptimizationVerificationAware;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.expression.function.aggregate.FirstDocId;
import org.elasticsearch.xpack.esql.optimizer.rules.PlanConsistencyChecker;
import org.elasticsearch.xpack.esql.plan.logical.ExecutesOn;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeExec;
import org.elasticsearch.xpack.esql.plan.physical.FetchExec;
import org.elasticsearch.xpack.esql.plan.physical.FieldExtractExec;
import org.elasticsearch.xpack.esql.plan.physical.MergeExec;
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
            checkFetchesReadDocumentsThatLeftTheirNode(optimizedPlan, failures);
            if (optimizedPlan.output().stream().anyMatch(EsQueryExec::isDocAttribute)) {
                failures.add(fail(optimizedPlan, "document identity [_doc] cannot be returned, found {}", optimizedPlan.output()));
            }
        }
    }

    /**
     * Rows carry their {@code _doc} across an exchange only for a {@link FetchExec} above it, which loads the documents
     * from the nodes that own them. The data nodes then keep the readers of those documents open and detach them, so the
     * documents still name their readers on the coordinator. A {@code _doc} that left its node is good for nothing else,
     * because only the node that read it can load the document.
     */
    private static void checkDocumentsLeaveTheirNodeOnlyForAFetch(PhysicalPlan plan, Failures failures) {
        Set<NameId> fetched = new HashSet<>();
        plan.forEachDown(FetchExec.class, fetch -> fetched.addAll(documentsReadBy(fetch)));
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

    /**
     * A {@link FetchExec} loads the documents of the rows the data nodes sent, so its {@code _doc} crosses an exchange
     * below it. The logical planner adds the fetch above a cut and relies on the mapper to put that exchange below it.
     */
    private static void checkFetchesReadDocumentsThatLeftTheirNode(PhysicalPlan plan, Failures failures) {
        plan.forEachDown(FetchExec.class, fetch -> {
            Set<NameId> documents = documentsReadBy(fetch);
            boolean crossed = fetch.child()
                .anyMatch(
                    node -> node instanceof ExchangeExec exchange
                        && exchange.output().stream().anyMatch(attribute -> documents.contains(attribute.id()))
                );
            if (crossed == false) {
                failures.add(fail(fetch, "[{}] must read a [_doc] that crossed an exchange", fetch.nodeString()));
            }
        });
    }

    /**
     * The ids of the {@code _doc} columns {@code fetch} reads, below it. A {@code FORK} merges the {@code _doc} of each
     * branch at the same position, and an aggregation can keep the first {@code _doc} of each group, so one fetch can read
     * the {@code _doc} of several exchanges.
     */
    private static Set<NameId> documentsReadBy(FetchExec fetch) {
        Set<NameId> documents = new HashSet<>();
        documents.add(fetch.doc().id());
        // parents come first, so the _doc a node reads is known before the nodes below it
        fetch.child().forEachDown(node -> {
            switch (node) {
                case MergeExec merge -> {
                    for (int position = 0; position < merge.output().size(); position++) {
                        if (documents.contains(merge.output().get(position).id())) {
                            for (PhysicalPlan branch : merge.children()) {
                                documents.add(branch.output().get(position).id());
                            }
                        }
                    }
                }
                case AggregateExec aggregate -> {
                    for (NamedExpression expression : aggregate.aggregates()) {
                        if (documents.contains(expression.id())
                            && expression instanceof Alias alias
                            && alias.child() instanceof FirstDocId firstDoc
                            && firstDoc.field() instanceof Attribute doc) {
                            documents.add(doc.id());
                        }
                    }
                }
                default -> {
                }
            }
        });
        return documents;
    }
}
