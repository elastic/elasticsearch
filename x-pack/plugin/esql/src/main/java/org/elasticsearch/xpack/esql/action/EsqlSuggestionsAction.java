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
 * <p>This is deliberately a {@code cluster:} action, not an {@code indices:data/read/*} one. The
 * current implementation (see {@link TransportEsqlSuggestionsAction}) only parses the query — it
 * never analyzes it or resolves indices, so it never reads mappings or is subject to per-index
 * field access control. Naming it as an index action would make it eligible for index-scoped
 * {@code read} privileges (it matches the {@code indices:data/read/*} pattern) while the request
 * doesn't implement {@link org.elasticsearch.action.IndicesRequest}, which trips an assertion in
 * {@code RBACEngine} for actions resolved index-name-only. Once the deferred data-node visit
 * lands (see the suggestions API spec, Step 8) and this action actually reads mappings/field
 * access control for a specific index, it should be revisited and likely turned into a proper
 * {@code indices:data/read/*} action carrying {@code IndicesRequest}. Until then it is granted by
 * the existing {@code monitor_esql} cluster privilege, matching {@link EsqlGetQueryAction} and
 * {@link EsqlListQueriesAction}.
 */
public class EsqlSuggestionsAction extends ActionType<EsqlSuggestionsResponse> {

    public static final EsqlSuggestionsAction INSTANCE = new EsqlSuggestionsAction();
    public static final String NAME = "cluster:monitor/xpack/esql/suggestions";

    private EsqlSuggestionsAction() {
        super(NAME);
    }
}
