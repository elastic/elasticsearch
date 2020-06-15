/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.execution.listener;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.xpack.eql.EqlIllegalArgumentException;
import org.elasticsearch.xpack.eql.session.Results;

import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.xpack.eql.execution.listener.RuntimeUtils.logSearchResponse;

public class BasicListener implements ActionListener<SearchResponse> {

    private static final Logger log = LogManager.getLogger(BasicListener.class);

    private final ActionListener<Results> listener;
    private final SearchRequest request;

    public BasicListener(ActionListener<Results> listener,
                               SearchRequest request) {

        this.listener = listener;
        this.request = request;
    }

    @Override
    public void onResponse(SearchResponse response) {
        try {
            ShardSearchFailure[] failures = response.getShardFailures();
            if (CollectionUtils.isEmpty(failures) == false) {
                listener.onFailure(new EqlIllegalArgumentException(failures[0].reason(), failures[0].getCause()));
            } else {
                handleResponse(response, listener);
            }
        } catch (Exception ex) {
            listener.onFailure(ex);
        }
    }

    private void handleResponse(SearchResponse response, ActionListener<Results> listener) {
        if (log.isTraceEnabled()) {
            logSearchResponse(response, log);
        }

        List<SearchHit> results = Arrays.asList(response.getHits().getHits());
        listener.onResponse(Results.fromHits(response.getTook(), results));
    }


    @Override
    public void onFailure(Exception ex) {
        listener.onFailure(ex);
    }
}