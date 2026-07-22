/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.action.support.ThreadedActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.DatasetMetadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.search.crossproject.CrossProjectModeDecider;
import org.elasticsearch.xpack.esql.action.EsqlResolveDatasetAction;
import org.elasticsearch.xpack.esql.datasources.DatasetRewriter.DatasetResolution;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;

import static org.elasticsearch.rest.RestUtils.REST_MASTER_TIMEOUT_DEFAULT;

/**
 * Owns the security round-trip for {@code FROM <dataset>}, the companion of {@link DatasetRewriter}: each FROM relation
 * the query reads is dispatched — one {@link EsqlResolveDatasetAction} per relation, with that relation's RAW patterns —
 * so the authorization engine read-authorizes the names (index {@code read} privilege) and rejects DLS/FLS-restricted
 * datasets, then narrows wildcard expansion to the authorized abstractions. The per-relation results feed a synchronous
 * {@link DatasetRewriter#rewrite} that turns authorized datasets into external relations.
 *
 * <p>Per-relation dispatch (rather than one request for the whole plan) is required because exclusion semantics are
 * per-relation: {@code -logs_test} in one FROM must not drop {@code logs_test} from a different FROM in the same plan.
 * Mirrors how {@code ViewResolver} routes each {@code UnresolvedRelation}'s raw patterns through
 * {@code EsqlResolveViewAction}.
 *
 * <p>When no dataset is registered in cluster state, or no FROM pattern can match a registered dataset, the listener
 * completes synchronously and no request is sent.
 */
public class DatasetResolver {

    private final Client client;
    private final Executor executor;
    private final CrossProjectModeDecider crossProjectModeDecider;

    public DatasetResolver(Client client, Executor executor, CrossProjectModeDecider crossProjectModeDecider) {
        this.client = client;
        this.executor = executor;
        this.crossProjectModeDecider = crossProjectModeDecider;
    }

    /**
     * Completes {@code listener} with the rewritten plan (or the untouched plan when no relation qualifies).
     * Authorization failures (DLS/FLS, and the {@code Unknown index} a rewrite raises for an explicit unauthorized
     * dataset) propagate as-is.
     *
     * <p>When federation is suppressed (see {@link Federation}) the rewrite is skipped entirely: the plan is returned
     * untouched, so a {@code FROM <dataset>} name flows into normal index resolution and errors as {@code Unknown index},
     * exactly as a nonexistent index would. No dataset lookup and no {@link EsqlResolveDatasetAction} dispatch happen.
     */
    public void replaceDatasets(LogicalPlan parsed, ProjectMetadata projectMetadata, ActionListener<LogicalPlan> listener) {
        replaceDatasets(parsed, projectMetadata, listener, Federation.isAvailable());
    }

    /**
     * Package-private overload that accepts the federation-enabled state as a parameter, allowing unit tests to exercise
     * the kill-switch branch without relying on the {@code static final} field in {@link Federation}.
     */
    void replaceDatasets(
        LogicalPlan parsed,
        ProjectMetadata projectMetadata,
        ActionListener<LogicalPlan> listener,
        boolean federationEnabled
    ) {
        // Federation suppressed: do not attempt any dataset resolution, so the feature is indistinguishable from one
        // that was never registered (the FROM <dataset> name resolves as an unknown index). The EsqlResolveDatasetAction
        // is also unregistered in this mode, so dispatching it here would fail; skipping is both correct and required.
        if (federationEnabled == false) {
            listener.onResponse(parsed);
            return;
        }

        // Cheap short-circuit: no datasets registered → no FROM can target one, so no dispatch and no walk cost on the
        // common path.
        Set<String> datasetNames = projectMetadata == null ? Set.of() : DatasetMetadata.get(projectMetadata).datasets().keySet();
        if (datasetNames.isEmpty()) {
            listener.onResponse(parsed);
            return;
        }

        // Collect the relations worth a round-trip: skip remote-prefixed (datasets are local-only, CCS sees the original
        // FROM) and skip any relation whose patterns could not match a registered dataset name (ordinary FROM <index>).
        List<UnresolvedRelation> relations = new ArrayList<>();
        parsed.forEachUp(UnresolvedRelation.class, r -> {
            List<String> patterns = DatasetRewriter.patternsOf(r);
            if (DatasetRewriter.hasRemotePattern(patterns)
                || DatasetRewriter.anyPatternCouldMatchDataset(patterns, datasetNames) == false) {
                return;
            }
            relations.add(r);
        });
        if (relations.isEmpty()) {
            listener.onResponse(parsed);
            return;
        }

        // One request per relation, chained sequentially via andThen so each result lands in the identity-keyed map
        // before the next dispatches. (forEachUp may visit the same relation instance more than once in a degenerate
        // tree; the map dedups by identity.)
        Map<UnresolvedRelation, DatasetResolution> resolutions = new IdentityHashMap<>();
        SubscribableListener<Void> chain = SubscribableListener.newForked(l -> l.onResponse(null));
        for (UnresolvedRelation relation : relations) {
            chain = chain.andThen((l, ignored) -> {
                if (resolutions.containsKey(relation)) {
                    l.onResponse(null);
                    return;
                }
                var request = new EsqlResolveDatasetAction.Request(
                    REST_MASTER_TIMEOUT_DEFAULT,
                    DatasetRewriter.patternsOf(relation).toArray(String[]::new)
                );
                client.execute(
                    EsqlResolveDatasetAction.TYPE,
                    request,
                    new ThreadedActionListener<>(executor, l.delegateFailureAndWrap((delegate, response) -> {
                        resolutions.put(
                            relation,
                            new DatasetResolution(response.datasets(), response.nonDatasetNames(), response.explicitUnauthorized())
                        );
                        delegate.onResponse(null);
                    }))
                );
            });
        }
        // Remote-dataset detection rides the field-caps rail (EsqlResolveFieldsAction, surfaced as
        // RemoteDatasetNotSupportedException). This resolver only does the LOCAL rewrite + read-authz; under CPS it
        // additionally preserves the remote half of a matched dataset (see DatasetRewriter.rewrite, crossProjectEnabled=true).
        boolean crossProjectEnabled = crossProjectModeDecider.crossProjectEnabled();
        chain.andThenApply(ignored -> DatasetRewriter.rewrite(parsed, projectMetadata, resolutions, crossProjectEnabled))
            .addListener(listener);
    }
}
