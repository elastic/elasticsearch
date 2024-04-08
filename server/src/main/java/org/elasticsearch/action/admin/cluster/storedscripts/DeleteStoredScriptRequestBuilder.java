/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.storedscripts;

import org.elasticsearch.action.support.master.AcknowledgedRequestBuilder;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.ElasticsearchClient;

public class DeleteStoredScriptRequestBuilder extends AcknowledgedRequestBuilder<
    DeleteStoredScriptRequest,
    AcknowledgedResponse,
    DeleteStoredScriptRequestBuilder> {

    public DeleteStoredScriptRequestBuilder(ElasticsearchClient client) {
        super(client, TransportDeleteStoredScriptAction.TYPE, new DeleteStoredScriptRequest());
    }

    public DeleteStoredScriptRequestBuilder setId(String id) {
        request.id(id);

        return this;
    }

}
