/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.reindex;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.script.Script;

public abstract class AbstractBulkIndexByScrollRequestBuilder<
                Request extends AbstractBulkIndexByScrollRequest<Request>,
                Self extends AbstractBulkIndexByScrollRequestBuilder<Request, Self>>
        extends AbstractBulkByScrollRequestBuilder<Request, Self> {

    protected AbstractBulkIndexByScrollRequestBuilder(ElasticsearchClient client, ActionType<BulkByScrollResponse> action,
                                                      SearchRequestBuilder search, Request request) {
        super(client, action, search, request);
    }

    /**
     * Script to modify the documents before they are processed.
     */
    public Self script(Script script) {
        request.setScript(script);
        return self();
    }
}
