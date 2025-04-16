/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.reindex;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.internal.ElasticsearchClient;
import org.elasticsearch.script.Script;

public abstract class AbstractBulkIndexByScrollRequestBuilder<
    Request extends AbstractBulkIndexByScrollRequest<Request>,
    Self extends AbstractBulkIndexByScrollRequestBuilder<Request, Self>> extends AbstractBulkByScrollRequestBuilder<Request, Self> {
    private Script script;

    protected AbstractBulkIndexByScrollRequestBuilder(
        ElasticsearchClient client,
        ActionType<BulkByScrollResponse> action,
        SearchRequestBuilder search
    ) {
        super(client, action, search);
    }

    /**
     * Script to modify the documents before they are processed.
     */
    public Self script(Script script) {
        this.script = script;
        return self();
    }

    @Override
    public void apply(Request request) {
        super.apply(request);
        if (script != null) {
            request.setScript(script);
        }
    }
}
