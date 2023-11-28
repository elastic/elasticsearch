/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.admin.indices.template.get;

import org.elasticsearch.action.support.master.MasterNodeReadOperationRequestBuilder;
import org.elasticsearch.client.internal.ElasticsearchClient;

public class GetIndexTemplatesRequestBuilder extends MasterNodeReadOperationRequestBuilder<
    GetIndexTemplatesRequest,
    GetIndexTemplatesResponse,
    GetIndexTemplatesRequestBuilder> {

    public GetIndexTemplatesRequestBuilder(ElasticsearchClient client, GetIndexTemplatesAction action, String... names) {
        super(client, action, new GetIndexTemplatesRequest(names));
    }
}
