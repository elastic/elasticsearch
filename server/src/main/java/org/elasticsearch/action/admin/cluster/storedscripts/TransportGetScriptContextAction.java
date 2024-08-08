/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.admin.cluster.storedscripts;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.script.ScriptContextInfo;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

import java.util.Set;

public class TransportGetScriptContextAction extends HandledTransportAction<GetScriptContextRequest, GetScriptContextResponse> {

    private final ScriptService scriptService;

    @Inject
    public TransportGetScriptContextAction(TransportService transportService, ActionFilters actionFilters, ScriptService scriptService) {
        super(
            GetScriptContextAction.NAME,
            transportService,
            actionFilters,
            GetScriptContextRequest::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.scriptService = scriptService;
    }

    @Override
    protected void doExecute(Task task, GetScriptContextRequest request, ActionListener<GetScriptContextResponse> listener) {
        Set<ScriptContextInfo> contexts = scriptService.getContextInfos();
        listener.onResponse(new GetScriptContextResponse(contexts));
    }
}
