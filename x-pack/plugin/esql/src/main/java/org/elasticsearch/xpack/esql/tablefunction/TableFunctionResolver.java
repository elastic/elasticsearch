/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.tablefunction;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ThreadedActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.xpack.esql.action.EsqlAuthorizeDatasourceAction;
import org.elasticsearch.xpack.esql.action.PlanningCpuTracker;
import org.elasticsearch.xpack.esql.datasources.Federation;
import org.elasticsearch.xpack.esql.datasources.FederationLicense;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedTableFunctionRelation;
import org.elasticsearch.xpack.esql.session.EsqlLicenseChecker;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;

import static org.elasticsearch.rest.RestUtils.REST_MASTER_TIMEOUT_DEFAULT;

/**
 * Owns the security round-trip for a table function in a source list, and the rewrite that follows it: every
 * {@link UnresolvedTableFunctionRelation} the parser left in the plan becomes the leaf that actually reads.
 * <p>
 * The companion of {@code DatasetResolver}, and deliberately not the same gate. A registered dataset is an index
 * abstraction, so the caller's index {@code read} privilege on its name governs it and the dataset resolve action is
 * what applies that. A data source is in no index namespace, so no index privilege says anything about it; what
 * governs it is {@code read} under {@code global.data_source}, applied by dispatching
 * {@link EsqlAuthorizeDatasourceAction} in the caller's own thread context.
 * <p>
 * That action is a cluster action, so the decision is all-or-nothing and the security layer makes it before the
 * handler runs: a caller whose grant does not cover every name asked for never reaches the handler, and sees the
 * authorization layer's own refusal. What the handler answers is therefore only which of those names this project has
 * registered, and a name missing from its answer means exactly one thing — not registered here — which is why that
 * case is reported the way the registration API reports it.
 * <p>
 * One round-trip for the whole plan, not one per leaf. The dataset gate is per-relation because exclusions are
 * per-relation — {@code -logs_test} in one FROM must not narrow another — and a table function has no exclusions and
 * no wildcards at all: {@code TableFunction#validate} has already refused anything but a single exact name, so the
 * names from every call in the query compose into one request with nothing to disambiguate.
 * <p>
 * Federation availability and the Enterprise licence are checked here for the reason the dataset path checks them: a
 * data source is federation whichever syntax reaches it. Registering a dataset over one is licence-gated
 * ({@code TransportPutDatasetAction}), so reading one inline has to be too, or the licence would turn on which of the
 * two syntaxes was used.
 */
public class TableFunctionResolver {

    private final Client client;
    private final Executor executor;
    private final boolean federationAvailable;
    private final FederationLicense federationLicense;
    private final TableFunctionRegistry registry;

    /**
     * @param executor where the decision is delivered. The authorize action can complete on another thread (the
     *                 security filter's role resolution, a cluster-state block wait), so the continuation is moved
     *                 back off it, as {@code ExternalSourceAuthorizer} does.
     * @param federationAvailable resolved once by the caller ({@link Federation#isAvailable}), since both of its
     *                            levers are read at startup
     * @param federationLicense queried per query rather than captured, so the check reflects the current licence
     * @param registry the functions this node plans against, each already holding whatever it needs of its own
     */
    public TableFunctionResolver(
        Client client,
        Executor executor,
        boolean federationAvailable,
        FederationLicense federationLicense,
        TableFunctionRegistry registry
    ) {
        this.client = client;
        this.executor = executor;
        this.federationAvailable = federationAvailable;
        this.federationLicense = federationLicense;
        this.registry = registry;
    }

    /**
     * Completes {@code listener} with the plan whose table functions have become reading leaves, or with the plan
     * untouched when it has none — which costs one walk and no round-trip, so an ordinary query pays almost nothing.
     * <p>
     * No parent task is set on the authorize request, following the dataset gate: it is a local cluster-state read on
     * a direct executor, so there is nothing long-running for a cancellation to reach.
     */
    public void resolveTableFunctions(LogicalPlan parsed, ProjectMetadata projectMetadata, ActionListener<LogicalPlan> listener) {
        List<UnresolvedTableFunctionRelation> leaves = new ArrayList<>();
        parsed.forEachDown(node -> {
            if (node instanceof UnresolvedTableFunctionRelation leaf) {
                leaves.add(leaf);
            }
        });
        if (leaves.isEmpty()) {
            listener.onResponse(parsed);
            return;
        }

        // Resolved from the registry rather than carried on the call: the parser already resolved the name to raise
        // an unknown-function error at the right position, and a plan node holding a registry entry would make the
        // node's identity depend on it.
        Set<String> dataSourceNames = new LinkedHashSet<>();
        for (UnresolvedTableFunctionRelation leaf : leaves) {
            TableFunctionCall call = leaf.call();
            // Resolving the name, then the shape, then whatever else the function requires — all of it before any
            // authorization, so a malformed call is answered by its own diagnostic rather than by a round-trip.
            TableFunction function = registry.resolve(call.source(), call.name());
            function.signature().check(call);
            function.validate(call);
            dataSourceNames.addAll(function.dataSourceNames(call));
        }

        // A call that reads no data source is bound here and asks the cluster nothing. The three gates below are
        // properties of reading a data source, not of being a table function, so none of them applies: there is no
        // name to authorize, nothing federated to be unavailable, and no federation feature to licence. Keeping them
        // keyed on the names rather than on the facility is what lets a generator, or anything built from the request
        // itself, join the registry without the facility having to learn about it.
        if (dataSourceNames.isEmpty()) {
            // completeWith, not onResponse: the rewrite runs a function's own binding and can throw, and on this path
            // there is no delegateFailureAndWrap above us to turn that into a failed listener.
            ActionListener.completeWith(listener, () -> rewrite(parsed, leaves, projectMetadata));
            return;
        }

        // Unlike a dataset name, a call has nothing to fall through to when federation is off: it cannot be read as an
        // index pattern, so the plan cannot simply be handed back untouched.
        if (federationAvailable == false) {
            listener.onFailure(Federation.notAvailableException());
            return;
        }

        // Before the round-trip, so a non-Enterprise cluster answers with the licence rather than with whether the
        // named data source happens to exist. Usage is recorded only once a read is actually built, below.
        if (federationLicense.isAllowedWithoutTracking() == false) {
            listener.onFailure(EsqlLicenseChecker.invalidLicenseForFederationException(federationLicense.get()));
            return;
        }

        if (projectMetadata == null) {
            listener.onFailure(notRegistered(dataSourceNames.iterator().next()));
            return;
        }

        EsqlAuthorizeDatasourceAction.Request request = new EsqlAuthorizeDatasourceAction.Request(
            REST_MASTER_TIMEOUT_DEFAULT,
            dataSourceNames.toArray(String[]::new)
        );
        client.execute(
            EsqlAuthorizeDatasourceAction.TYPE,
            request,
            new ThreadedActionListener<>(
                executor,
                // Metering is re-established across the hand-off, as the dataset gate does it: the rewrite below runs
                // the validator and the mapping round-trip, which is planning CPU and would otherwise go uncounted.
                PlanningCpuTracker.inheritMeteredCpu(listener.delegateFailureAndWrap((delegate, response) -> {
                    for (String name : dataSourceNames) {
                        if (response.dataSources().contains(name) == false) {
                            delegate.onFailure(notRegistered(name));
                            return;
                        }
                    }
                    LogicalPlan rewritten = rewrite(parsed, leaves, projectMetadata);
                    federationLicense.isAllowed(); // records feature-usage telemetry; the licence is checked above
                    delegate.onResponse(rewritten);
                }))
            )
        );
    }

    /**
     * Binds each leaf and swaps it into the plan. Keyed by identity rather than equality: two calls written
     * identically at different offsets are already unequal, because a {@link TableFunctionCall} carries its
     * {@code Source} — so equality would work today, and would start collapsing leaves the moment a call's identity
     * stopped including its position.
     */
    private LogicalPlan rewrite(LogicalPlan parsed, List<UnresolvedTableFunctionRelation> leaves, ProjectMetadata projectMetadata) {
        TableFunctionContext context = new TableFunctionContext(projectMetadata);
        Map<UnresolvedTableFunctionRelation, LogicalPlan> bound = new IdentityHashMap<>(leaves.size());
        for (UnresolvedTableFunctionRelation leaf : leaves) {
            TableFunctionCall call = leaf.call();
            TableFunction function = registry.resolve(call.source(), call.name());
            bound.put(leaf, function.toRelation(call, leaf.metadataFields(), context));
        }
        return parsed.transformDown(UnresolvedTableFunctionRelation.class, leaf -> {
            LogicalPlan relation = bound.get(leaf);
            assert relation != null : "a leaf reached the rewrite that the authorization walk did not see";
            return relation;
        });
    }

    /**
     * The refusal for a data source this project has not registered. The status and wording the registration API
     * gives, because by the time this can be raised the caller is known to be authorized for the name: an
     * unauthorized one is refused by the security layer before the handler runs, so this reveals nothing the caller
     * is not entitled to.
     */
    private static ResourceNotFoundException notRegistered(String dataSourceName) {
        return new ResourceNotFoundException("data source [{}] not found", dataSourceName);
    }
}
