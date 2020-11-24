/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.example.actions;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.common.inject.Binding;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.TypeLiteral;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.List;

public class TransportGetActionsAction extends HandledTransportAction<GetActionsRequest, GetActionsResponse> {

    private final Injector injector;

    @Inject
    public TransportGetActionsAction(TransportService transportService, ActionFilters actionFilters, Injector injector) {
        super(GetActionsAction.NAME, transportService, actionFilters, GetActionsRequest::new);
        this.injector = injector;
    }

    @SuppressWarnings("rawtypes")
    @Override
    protected void doExecute(Task task, GetActionsRequest request, ActionListener<GetActionsResponse> listener) {
        final List<Binding<TransportAction>> bindings = injector.findBindingsByType(TypeLiteral.get(TransportAction.class));

        final List<String> allActionNames = new ArrayList<>(bindings.size());
        for (final Binding<TransportAction> binding : bindings) {
            allActionNames.add(binding.getProvider().get().actionName);
        }
        listener.onResponse(new GetActionsResponse(allActionNames));
    }
}
