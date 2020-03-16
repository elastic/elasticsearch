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


package org.elasticsearch.client.asyncsearch;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.Validatable;
import org.elasticsearch.common.unit.TimeValue;

import java.util.Objects;

/**
 * A request to track asynchronously the progress of a search against one or more indices.
 */
public class SubmitAsyncSearchRequest implements Validatable {

    private TimeValue waitForCompletion;
    private Boolean cleanOnCompletion;
    private TimeValue keepAlive;
    private final SearchRequest searchRequest;

    /**
     * Create a request to submit an async search.
     * Target indices, queries and all other search related options should be set on
     * the input {@link SearchRequest}.
     * @param searchRequest the actual search request to submit
     */
    public SubmitAsyncSearchRequest(SearchRequest searchRequest) {
        this.searchRequest = new SearchRequest(searchRequest);
        this.searchRequest.setCcsMinimizeRoundtrips(false);
        this.searchRequest.setPreFilterShardSize(1);
        this.searchRequest.setBatchedReduceSize(5);
        this.searchRequest.requestCache(true);
    }


    /**
     * Get the {@link SearchRequest} that this submit request wraps
     */
    public SearchRequest getSearchRequest() {
        return searchRequest;
    }

    /**
     * Get the minimum time that the request should wait before returning a partial result (defaults to 1 second).
     */
    public TimeValue getWaitForCompletion() {
        return waitForCompletion;
    }

    /**
     * Sets the minimum time that the request should wait before returning a partial result (defaults to 1 second).
     */
    public void setWaitForCompletion(TimeValue waitForCompletion) {
        this.waitForCompletion = waitForCompletion;
    }

    /**
     * Returns whether the resource resource should be removed on completion or failure (defaults to true).
     */
    public Boolean isCleanOnCompletion() {
        return cleanOnCompletion;
    }

    /**
     * Determines if the resource should be removed on completion or failure (defaults to true).
     */
    public void setCleanOnCompletion(boolean cleanOnCompletion) {
        this.cleanOnCompletion = cleanOnCompletion;
    }

    /**
     * Get the amount of time after which the result will expire (defaults to 5 days).
     */
    public TimeValue getKeepAlive() {
        return keepAlive;
    }

    /**
     * Sets the amount of time after which the result will expire (defaults to 5 days).
     */
    public void setKeepAlive(TimeValue keepAlive) {
        this.keepAlive = keepAlive;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SubmitAsyncSearchRequest request = (SubmitAsyncSearchRequest) o;
        return Objects.equals(getSearchRequest(), request.getSearchRequest())
                && Objects.equals(getKeepAlive(), request.getKeepAlive())
                && Objects.equals(getWaitForCompletion(), request.getWaitForCompletion())
                && Objects.equals(isCleanOnCompletion(), request.isCleanOnCompletion());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getSearchRequest(), getKeepAlive(), getWaitForCompletion(), isCleanOnCompletion());
    }
}
