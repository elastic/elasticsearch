/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.reindex;

import org.elasticsearch.action.bulk.BulkItemResponse.Failure;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.support.RestToXContentListener;

/**
 * Just like RestToXContentListener but will return higher than 200 status if
 * there are any failures.
 */
public class BulkIndexByScrollResponseContentListener<R extends BulkIndexByScrollResponse> extends RestToXContentListener<R> {
    public BulkIndexByScrollResponseContentListener(RestChannel channel) {
        super(channel);
    }

    @Override
    protected RestStatus getStatus(R response) {
        RestStatus status = RestStatus.OK;
        for (Failure failure : response.getIndexingFailures()) {
            if (failure.getStatus().getStatus() > status.getStatus()) {
                status = failure.getStatus();
            }
        }
        return status;
    }
}
