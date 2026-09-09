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
 * <p>This is an {@code indices:data/read/*} action, not a {@code cluster:} one: {@link
 * TransportEsqlSuggestionsAction} performs index resolution (parse, view resolution, dataset resolution,
 * analyze, optimize) on every request that doesn't fall back to the remote-qualified/coordinator-only
 * path, so it is index-scoped from security's point of view. The action name matches the {@code
 * indices:data/read/*} wildcard pattern already covered by {@code IndexPrivilege}'s {@code
 * READ_AUTOMATON}/{@code READ_FAILURE_STORE_AUTOMATON}, so no new privilege-automaton entry is needed.
 *
 * <p>{@link EsqlSuggestionsRequest} implements {@link org.elasticsearch.action.CompositeIndicesRequest}
 * (a marker only, no declared {@code indices()}), the same way {@code EsqlQueryRequest} does, and {@link
 * #NAME} is in {@code RBACEngine#shouldAuthorizeIndexActionNameOnly}'s explicit switch list alongside
 * {@code indices:data/read/esql}/{@code esql/compute}: RBAC checks only the action-name privilege up
 * front, and the real per-index enforcement happens later — in dataset/field-caps resolution (already
 * security-aware) for the coordinator-only path, and in the hot-tier value-sampling path's own explicit
 * FLS/DLS gate for the narrow slice that reads a raw {@code TermsEnum}. A plain {@code IndicesRequest}
 * declaring statically-parsed {@code FROM} targets was tried first, but a bare text parse cannot see
 * through view expansion or dataset resolution — a {@code FROM some_view} may resolve to entirely
 * different underlying indices than its name suggests — so a declared-up-front index set could be wrong
 * in either direction. See the suggestions API spec for the full reasoning.
 */
public class EsqlSuggestionsAction extends ActionType<EsqlSuggestionsResponse> {

    public static final EsqlSuggestionsAction INSTANCE = new EsqlSuggestionsAction();
    public static final String NAME = "indices:data/read/esql/suggestions";

    private EsqlSuggestionsAction() {
        super(NAME);
    }
}
