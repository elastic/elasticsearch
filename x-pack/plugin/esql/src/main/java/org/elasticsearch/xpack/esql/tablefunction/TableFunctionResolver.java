/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.tablefunction;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ThreadedActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.esql.action.EsqlAuthorizeDatasourceAction;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourceValidator;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedTableFunctionRelation;

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
 * {@link EsqlAuthorizeDatasourceAction} in the caller's own thread context. The security layer evaluates the
 * privilege before the handler runs, and the handler reports which of the names this project actually has.
 * <p>
 * One round-trip for the whole plan, not one per leaf. The dataset gate is per-relation because exclusions are
 * per-relation — {@code -logs_test} in one FROM must not narrow another — and a table function has no exclusions and
 * no wildcards at all: {@code TableFunction#validate} has already refused anything but a single exact name, so the
 * names from every call in the query compose into one request with nothing to disambiguate.
 * <p>
 * A name the caller may read but this project does not have, and a name the caller may not read, both come back
 * absent and draw one refusal, so neither answer tells a prober which of the two happened.
 */
public class TableFunctionResolver {

    private final Client client;
    private final Executor executor;
    private final Map<String, DataSourceValidator> validatorsByType;

    /**
     * @param executor where the decision is delivered. The authorize action can complete on another thread (the
     *                 security filter's role resolution, a cluster-state block wait), so the continuation is moved
     *                 back off it, as {@code ExternalSourceAuthorizer} does.
     * @param validatorsByType the registration API's own per-type validators, so an inline definition meets the bar
     *                         a registered one does
     */
    public TableFunctionResolver(Client client, Executor executor, Map<String, DataSourceValidator> validatorsByType) {
        this.client = client;
        this.executor = executor;
        this.validatorsByType = Map.copyOf(validatorsByType);
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
        if (projectMetadata == null) {
            listener.onFailure(notAuthorized(leaves.get(0).call().name()));
            return;
        }

        // Resolved from the registry rather than carried on the call: the parser already resolved the name to raise
        // an unknown-function error at the right position, and a plan node holding a registry entry would make the
        // node's identity depend on it.
        Set<String> dataSourceNames = new LinkedHashSet<>();
        for (UnresolvedTableFunctionRelation leaf : leaves) {
            TableFunctionCall call = leaf.call();
            dataSourceNames.addAll(TableFunctionRegistry.INSTANCE.resolve(call.source(), call.name()).dataSourceNames(call));
        }
        assert dataSourceNames.isEmpty() == false : "a table function that reads no data source needs no authorization path";

        EsqlAuthorizeDatasourceAction.Request request = new EsqlAuthorizeDatasourceAction.Request(
            REST_MASTER_TIMEOUT_DEFAULT,
            dataSourceNames.toArray(String[]::new)
        );
        client.execute(
            EsqlAuthorizeDatasourceAction.TYPE,
            request,
            new ThreadedActionListener<>(executor, true, listener.delegateFailureAndWrap((delegate, response) -> {
                for (String name : dataSourceNames) {
                    if (response.dataSources().contains(name) == false) {
                        delegate.onFailure(notAuthorized(name));
                        return;
                    }
                }
                delegate.onResponse(rewrite(parsed, leaves, projectMetadata));
            }))
        );
    }

    /**
     * Binds each leaf and swaps it into the plan. Built before the walk and keyed by identity, because two calls that
     * are written identically are equal and must still be replaced by their own relation rather than one of them
     * twice — the relations differ in nothing today, which is exactly why relying on that would be a trap later.
     */
    private LogicalPlan rewrite(LogicalPlan parsed, List<UnresolvedTableFunctionRelation> leaves, ProjectMetadata projectMetadata) {
        TableFunctionContext context = new TableFunctionContext(projectMetadata, validatorsByType);
        Map<UnresolvedTableFunctionRelation, LogicalPlan> bound = new IdentityHashMap<>(leaves.size());
        for (UnresolvedTableFunctionRelation leaf : leaves) {
            TableFunctionCall call = leaf.call();
            TableFunction function = TableFunctionRegistry.INSTANCE.resolve(call.source(), call.name());
            bound.put(leaf, function.toRelation(call, leaf.metadataFields(), context));
        }
        return parsed.transformDown(UnresolvedTableFunctionRelation.class, leaf -> {
            LogicalPlan relation = bound.get(leaf);
            assert relation != null : "a leaf reached the rewrite that the authorization walk did not see";
            return relation;
        });
    }

    /**
     * The one refusal for a data source the caller cannot read here, whether it is unregistered, outside the grant or
     * not registered at all. {@code 403} rather than the dataset gate's {@code 400}: a dataset name that resolves to
     * nothing is indistinguishable from a missing index and is reported as one, while a data source is named
     * explicitly by a syntax that means nothing else, so there is nothing for it to be mistaken for.
     */
    private static ElasticsearchStatusException notAuthorized(String dataSourceName) {
        return new ElasticsearchStatusException("data source [{}] is not available to this query", RestStatus.FORBIDDEN, dataSourceName);
    }
}
