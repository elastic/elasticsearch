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
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Validatable;
import org.elasticsearch.client.ValidationException;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.util.Objects;
import java.util.Optional;

/**
 * A request to track asynchronously the progress of a search against one or more indices.
 */
public class SubmitAsyncSearchRequest implements Validatable {

    public static long MIN_KEEP_ALIVE = TimeValue.timeValueMinutes(1).millis();

    private TimeValue waitForCompletion;
    private Boolean cleanOnCompletion;
    private TimeValue keepAlive;
    private final SearchRequest searchRequest;

    /**
     * Creates a new request
     */
    public SubmitAsyncSearchRequest(SearchSourceBuilder source, String... indices) {
        this.searchRequest = new SearchRequest(indices, source);
        searchRequest.setCcsMinimizeRoundtrips(false);
        searchRequest.setPreFilterShardSize(1);
        searchRequest.setBatchedReduceSize(5);
        searchRequest.requestCache(true);
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

    // setters for request parameters of the wrapped SearchRequest
    /**
     * Set the routing values to control the shards that the search will be executed on.
     * A comma separated list of routing values to control the shards the search will be executed on.
     */
    public void routing(String routing) {
        this.searchRequest.routing(routing);
    }

    /**
     * Set the routing values to control the shards that the search will be executed on.
     */
    public void routing(String... routings) {
        this.searchRequest.routing(routings);
    }

    /**
     * Sets the preference to execute the search. Defaults to randomize across shards. Can be set to
     * {@code _local} to prefer local shards or a custom value, which guarantees that the same order
     * will be used across different requests.
     */
    public void preference(String preference) {
        this.searchRequest.preference(preference);
    }

    /**
     * Specifies what type of requested indices to ignore and how to deal with indices wildcard expressions.
     */
    public void indicesOptions(IndicesOptions indicesOptions) {
        this.searchRequest.indicesOptions(indicesOptions);
    }

    /**
     * The search type to execute, defaults to {@link SearchType#DEFAULT}.
     */
    public void searchType(SearchType searchType) {
        this.searchRequest.searchType(searchType);
    }

    /**
     * Sets if this request should allow partial results. (If method is not called,
     * will default to the cluster level setting).
     */
    public void allowPartialSearchResults(boolean allowPartialSearchResults) {
        this.searchRequest.allowPartialSearchResults(allowPartialSearchResults);
    }

    @Override
    public Optional<ValidationException> validate() {
        final ValidationException validationException = new ValidationException();
        if (searchRequest.scroll() != null) {
            validationException.addValidationError("[scroll] queries are not supported");
        }
        if (searchRequest.isSuggestOnly()) {
            validationException.addValidationError("suggest-only queries are not supported");
        }
        if (searchRequest.isCcsMinimizeRoundtrips()) {
            validationException.addValidationError("[ccs_minimize_roundtrips] is not supported on async search queries");
        }
        if (keepAlive != null && keepAlive.getMillis() < MIN_KEEP_ALIVE) {
            validationException.addValidationError("[keep_alive] must be greater than 1 minute, got: " + keepAlive.toString());
        }
        if (validationException.validationErrors().isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(validationException);
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
