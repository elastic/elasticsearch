/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.execution.search;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.xpack.eql.EqlIllegalArgumentException;
import org.elasticsearch.xpack.eql.execution.payload.SearchResponsePayload;
import org.elasticsearch.xpack.eql.session.Payload;

import static org.elasticsearch.xpack.eql.execution.search.RuntimeUtils.logSearchResponse;

public class BasicListener implements ActionListener<SearchResponse> {

    private static final Logger log = RuntimeUtils.QUERY_LOG;

    private final ActionListener<Payload> listener;

    public BasicListener(ActionListener<Payload> listener) {
        this.listener = listener;
    }

    @Override
    public void onResponse(SearchResponse response) {
        try {
            ShardSearchFailure[] failures = response.getShardFailures();
            if (CollectionUtils.isEmpty(failures) == false) {
                listener.onFailure(new EqlIllegalArgumentException(failures[0].reason(), failures[0].getCause()));
            } else {
                if (log.isTraceEnabled()) {
                    logSearchResponse(response, log);
                }
                listener.onResponse(new SearchResponsePayload(response));
            }
        } catch (Exception ex) {
            onFailure(ex);
        }
    }

    @Override
    public void onFailure(Exception ex) {
        listener.onFailure(ex);
    }
}