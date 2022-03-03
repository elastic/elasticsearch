/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.search;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchShardTarget;

/**
 * A base action listener that ensures shard target and shard index is set on all responses
 * received by this listener.
 */
abstract class SearchActionListener<T extends SearchPhaseResult> implements ActionListener<T> {

    final int requestIndex;
    private final SearchShardTarget searchShardTarget;

    protected SearchActionListener(SearchShardTarget searchShardTarget, int shardIndex) {
        assert shardIndex >= 0 : "shard index must be positive";
        this.searchShardTarget = searchShardTarget;
        this.requestIndex = shardIndex;
    }

    @Override
    public final void onResponse(T response) {
        response.setShardIndex(requestIndex);
        setSearchShardTarget(response);
        innerOnResponse(response);
    }

    protected void setSearchShardTarget(T response) { // some impls need to override this
        response.setSearchShardTarget(searchShardTarget);
    }

    protected abstract void innerOnResponse(T response);
}
