/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.synonyms;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.synonyms.SynonymsManagementAPIService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

public class TransportPutSynonymRuleAction extends HandledTransportAction<PutSynonymRuleAction.Request, SynonymUpdateResponse> {

    private final SynonymsManagementAPIService synonymsManagementAPIService;

    @Inject
    public TransportPutSynonymRuleAction(TransportService transportService, ActionFilters actionFilters, Client client) {
        super(PutSynonymRuleAction.NAME, transportService, actionFilters, PutSynonymRuleAction.Request::new);

        this.synonymsManagementAPIService = new SynonymsManagementAPIService(client);
    }

    @Override
    protected void doExecute(Task task, PutSynonymRuleAction.Request request, ActionListener<SynonymUpdateResponse> listener) {
        synonymsManagementAPIService.putSynonymRule(
            request.synonymsSetId(),
            request.synonymRule(),
            listener.map(SynonymUpdateResponse::new)
        );
    }
}
