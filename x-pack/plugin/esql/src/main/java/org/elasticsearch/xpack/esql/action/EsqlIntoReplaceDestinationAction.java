/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.index.TransportIndexAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.transport.TransportService;

/**
 * As {@link EsqlIntoDestinationAction}, for an {@code INTO} that replaces documents. A bulk sends {@code index} requests for it, which
 * the security filter authorizes as {@code indices:data/write/index:op_type/index}, so {@code create_doc} does not allow them, and
 * the caller must be denied before anything is reported about the destination, as for any other write. The name copies
 * {@code AuthorizationService.IMPLIED_INDEX_ACTION}, which is private to the security plugin.
 */
public class EsqlIntoReplaceDestinationAction extends EsqlIntoDestinationAction {
    public static final String NAME = TransportIndexAction.NAME + ":op_type/index";
    public static final ActionType<Response> TYPE = new ActionType<>(NAME);

    @Inject
    public EsqlIntoReplaceDestinationAction(TransportService transportService, ActionFilters actionFilters) {
        super(NAME, transportService, actionFilters);
    }
}
