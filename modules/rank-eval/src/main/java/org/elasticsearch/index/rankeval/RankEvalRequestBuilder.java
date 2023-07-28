/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.rankeval;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.client.internal.ElasticsearchClient;

public class RankEvalRequestBuilder extends ActionRequestBuilder<RankEvalRequest, RankEvalResponse> {

    public RankEvalRequestBuilder(ElasticsearchClient client, ActionType<RankEvalResponse> action, RankEvalRequest request) {
        super(client, action, request);
    }

    public void setRankEvalSpec(RankEvalSpec spec) {
        this.request.setRankEvalSpec(spec);
    }

}
