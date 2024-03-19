/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.reindex;

import org.elasticsearch.action.bulk.BulkItemResponse.Failure;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.ScrollableHitSource.SearchFailure;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;

import java.util.Map;

/**
 * RestBuilderListener that returns higher than 200 status if there are any failures and allows to set XContent.Params.
 */
public class BulkIndexByScrollResponseContentListener extends RestBuilderListener<BulkByScrollResponse> {

    private final Map<String, String> params;

    public BulkIndexByScrollResponseContentListener(RestChannel channel, Map<String, String> params) {
        super(channel);
        this.params = params;
    }

    @Override
    public RestResponse buildResponse(BulkByScrollResponse response, XContentBuilder builder) throws Exception {
        builder.startObject();
        response.toXContent(builder, new ToXContent.DelegatingMapParams(params, channel.request()));
        builder.endObject();
        return new RestResponse(getStatus(response), builder);
    }

    private static RestStatus getStatus(BulkByScrollResponse response) {
        /*
         * Return the highest numbered rest status under the assumption that higher numbered statuses are "more error" and thus more
         * interesting to the user.
         */
        RestStatus status = RestStatus.OK;
        if (response.isTimedOut()) {
            status = RestStatus.REQUEST_TIMEOUT;
        }
        for (Failure failure : response.getBulkFailures()) {
            if (failure.getStatus().getStatus() > status.getStatus()) {
                status = failure.getStatus();
            }
        }
        for (SearchFailure failure : response.getSearchFailures()) {
            RestStatus failureStatus = failure.getStatus();
            if (failureStatus.getStatus() > status.getStatus()) {
                status = failureStatus;
            }
        }
        return status;
    }
}
