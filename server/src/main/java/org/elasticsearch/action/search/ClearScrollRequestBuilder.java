/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;

import java.util.List;

public class ClearScrollRequestBuilder extends ActionRequestBuilder<ClearScrollRequest, ClearScrollResponse> {

    public ClearScrollRequestBuilder(ElasticsearchClient client, ClearScrollAction action) {
        super(client, action, new ClearScrollRequest());
    }

    public ClearScrollRequestBuilder setScrollIds(List<String> cursorIds) {
        request.setScrollIds(cursorIds);
        return this;
    }

    public ClearScrollRequestBuilder addScrollId(String cursorId) {
        request.addScrollId(cursorId);
        return this;
    }
}
