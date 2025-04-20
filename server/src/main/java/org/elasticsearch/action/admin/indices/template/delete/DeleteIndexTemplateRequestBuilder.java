/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.action.admin.indices.template.delete;

import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.MasterNodeOperationRequestBuilder;
import org.elasticsearch.client.internal.ElasticsearchClient;

public class DeleteIndexTemplateRequestBuilder extends MasterNodeOperationRequestBuilder<
    DeleteIndexTemplateRequest,
    AcknowledgedResponse,
    DeleteIndexTemplateRequestBuilder> {

    public DeleteIndexTemplateRequestBuilder(ElasticsearchClient client, String name) {
        super(client, TransportDeleteIndexTemplateAction.TYPE, new DeleteIndexTemplateRequest(name));
    }

    /**
     * Sets the name of the index template to delete.
     */
    public DeleteIndexTemplateRequestBuilder setName(String name) {
        request.name(name);
        return this;
    }
}
