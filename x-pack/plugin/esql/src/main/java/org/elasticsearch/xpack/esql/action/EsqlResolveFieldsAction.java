/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.RemoteClusterActionType;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesRequest;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.action.fieldcaps.TransportFieldCapabilitiesAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

/**
 * A fork of the field-caps API for ES|QL. This fork allows us to gradually introduce features and optimizations to this internal
 * API without risking breaking the external field-caps API. For now, this API delegates to the field-caps API, but gradually,
 * we will decouple this API completely from the field-caps.
 */
public class EsqlResolveFieldsAction extends HandledTransportAction<FieldCapabilitiesRequest, FieldCapabilitiesResponse> {
    public static final String NAME = "indices:data/read/esql/resolve_fields";
    public static final ActionType<FieldCapabilitiesResponse> TYPE = new ActionType<>(NAME);
    public static final RemoteClusterActionType<FieldCapabilitiesResponse> REMOTE_TYPE = new RemoteClusterActionType<>(
        NAME,
        FieldCapabilitiesResponse::new
    );

    private final TransportFieldCapabilitiesAction fieldCapsAction;

    @Inject
    public EsqlResolveFieldsAction(
        TransportService transportService,
        ActionFilters actionFilters,
        TransportFieldCapabilitiesAction fieldCapsAction
    ) {
        // TODO replace DIRECT_EXECUTOR_SERVICE when removing workaround for https://github.com/elastic/elasticsearch/issues/97916
        super(NAME, transportService, actionFilters, FieldCapabilitiesRequest::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.fieldCapsAction = fieldCapsAction;
    }

    @Override
    protected void doExecute(Task task, FieldCapabilitiesRequest request, final ActionListener<FieldCapabilitiesResponse> listener) {
        fieldCapsAction.executeRequest(task, request, REMOTE_TYPE, listener);
    }
}
