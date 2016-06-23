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

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.WriteConsistencyLevel;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;

public abstract class AbstractBulkByScrollRequestBuilder<
                Request extends AbstractBulkByScrollRequest<Request>,
                Self extends AbstractBulkByScrollRequestBuilder<Request, Self>>
        extends ActionRequestBuilder<Request, BulkIndexByScrollResponse, Self> {
    private final SearchRequestBuilder source;

    protected AbstractBulkByScrollRequestBuilder(ElasticsearchClient client,
            Action<Request, BulkIndexByScrollResponse, Self> action, SearchRequestBuilder source, Request request) {
        super(client, action, request);
        this.source = source;
    }

    protected abstract Self self();

    /**
     * The search used to find documents to process.
     */
    public SearchRequestBuilder source() {
        return source;
    }

    /**
     * Set the source indices.
     */
    public Self source(String... indices) {
        source.setIndices(indices);
        return self();
    }

    /**
     * Set the query that will filter the source. Just a convenience method for
     * easy chaining.
     */
    public Self filter(QueryBuilder filter) {
        source.setQuery(filter);
        return self();
    }

    /**
     * The maximum number of documents to attempt.
     */
    public Self size(int size) {
        request.setSize(size);
        return self();
    }

    /**
     * Should we version conflicts cause the action to abort?
     */
    public Self abortOnVersionConflict(boolean abortOnVersionConflict) {
        request.setAbortOnVersionConflict(abortOnVersionConflict);
        return self();
    }

    /**
     * Call refresh on the indexes we've written to after the request ends?
     */
    public Self refresh(boolean refresh) {
        request.setRefresh(refresh);
        return self();
    }

    /**
     * Timeout to wait for the shards on to be available for each bulk request.
     */
    public Self timeout(TimeValue timeout) {
        request.setTimeout(timeout);
        return self();
    }

    /**
     * Consistency level for write requests.
     */
    public Self consistency(WriteConsistencyLevel consistency) {
        request.setConsistency(consistency);
        return self();
    }

    /**
     * Initial delay after a rejection before retrying a bulk request. With the default maxRetries the total backoff for retrying rejections
     * is about one minute per bulk request. Once the entire bulk request is successful the retry counter resets.
     */
    public Self setRetryBackoffInitialTime(TimeValue retryBackoffInitialTime) {
        request.setRetryBackoffInitialTime(retryBackoffInitialTime);
        return self();
    }

    /**
     * Total number of retries attempted for rejections. There is no way to ask for unlimited retries.
     */
    public Self setMaxRetries(int maxRetries) {
        request.setMaxRetries(maxRetries);
        return self();
    }

    /**
     * Set the throttle for this request in sub-requests per second. {@link Float#POSITIVE_INFINITY} means set no throttle and that is the
     * default. Throttling is done between batches, as we start the next scroll requests. That way we can increase the scroll's timeout to
     * make sure that it contains any time that we might wait.
     */
    public Self setRequestsPerSecond(float requestsPerSecond) {
        request.setRequestsPerSecond(requestsPerSecond);
        return self();
    }

    /**
     * Should this task persist its result after it has finished?
     */
    public Self setShouldPersistResult(boolean shouldPersistResult) {
        request.setShouldPersistResult(shouldPersistResult);
        return self();
    }
}
