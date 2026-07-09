/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.ActionType;

/**
 * Transport action type for the cursor-aware autocomplete endpoint {@code POST /_esql/suggestions}.
 *
 * <p>This is an {@code indices:data/read/*} action, not a {@code cluster:} one: since the suggestions API
 * spec's Step 12, {@link TransportEsqlSuggestionsAction} genuinely performs index resolution (parse, view
 * resolution, dataset resolution, analyze, optimize) on every request that doesn't fall back to the
 * remote-qualified/coordinator-only path, so it is unambiguously index-scoped from security's point of
 * view. {@link EsqlSuggestionsRequest} implements {@link org.elasticsearch.action.IndicesRequest},
 * surfacing the {@code FROM} target(s) parsed from {@code query()}, so RBAC has a declared index set to
 * check privileges against independent of the query body. The action name matches the
 * {@code indices:data/read/*} wildcard pattern already covered by {@code IndexPrivilege}'s
 * {@code READ_AUTOMATON}/{@code READ_FAILURE_STORE_AUTOMATON}, so no new privilege-automaton entry is
 * needed.
 */
public class EsqlSuggestionsAction extends ActionType<EsqlSuggestionsResponse> {

    public static final EsqlSuggestionsAction INSTANCE = new EsqlSuggestionsAction();
    public static final String NAME = "indices:data/read/esql/suggestions";

    private EsqlSuggestionsAction() {
        super(NAME);
    }
}
