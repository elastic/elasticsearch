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

    private TimeValue waitForCompletionTimeout;
    private Boolean keepOnCompletion;
    private TimeValue keepAlive;
    private final SearchRequest searchRequest;
    // The following is optional and will only be sent down with the request if explicitely set by the user
    private Integer batchedReduceSize;

    /**
     * Creates a new request
     */
    public SubmitAsyncSearchRequest(SearchSourceBuilder source, String... indices) {
        this.searchRequest = new SearchRequest(indices, source);
    }

    /**
     * Get the target indices
     */
    public String[] getIndices() {
        return this.searchRequest.indices();
    }


    /**
     * Get the minimum time that the request should wait before returning a partial result (defaults to 1 second).
     */
    public TimeValue getWaitForCompletionTimeout() {
        return waitForCompletionTimeout;
    }

    /**
     * Sets the minimum time that the request should wait before returning a partial result (defaults to 1 second).
     */
    public void setWaitForCompletionTimeout(TimeValue waitForCompletionTimeout) {
        this.waitForCompletionTimeout = waitForCompletionTimeout;
    }

    /**
     * Returns whether the resource resource should be kept on completion or failure (defaults to false).
     */
    public Boolean isKeepOnCompletion() {
        return keepOnCompletion;
    }

    /**
     * Determines if the resource should be kept on completion or failure (defaults to false).
     */
    public void setKeepOnCompletion(boolean keepOnCompletion) {
        this.keepOnCompletion = keepOnCompletion;
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
     * Set the routing value to control the shards that the search will be executed on.
     * A comma separated list of routing values to control the shards the search will be executed on.
     */
    public void setRouting(String routing) {
        this.searchRequest.routing(routing);
    }

    /**
     * Set the routing values to control the shards that the search will be executed on.
     */
    public void setRoutings(String... routings) {
        this.searchRequest.routing(routings);
    }

    /**
     * Get the routing value to control the shards that the search will be executed on.
     */
    public String getRouting() {
        return this.searchRequest.routing();
    }

    /**
     * Sets the preference to execute the search. Defaults to randomize across shards. Can be set to
     * {@code _local} to prefer local shards or a custom value, which guarantees that the same order
     * will be used across different requests.
     */
    public void setPreference(String preference) {
        this.searchRequest.preference(preference);
    }

    /**
     * Get the preference to execute the search.
     */
    public String getPreference() {
        return this.searchRequest.preference();
    }

    /**
     * Specifies what type of requested indices to ignore and how to deal with indices wildcard expressions.
     */
    public void setIndicesOptions(IndicesOptions indicesOptions) {
        this.searchRequest.indicesOptions(indicesOptions);
    }

    /**
     * Get the indices Options.
     */
    public IndicesOptions getIndicesOptions() {
        return this.searchRequest.indicesOptions();
    }

    /**
     * The search type to execute, defaults to {@link SearchType#DEFAULT}.
     */
    public void setSearchType(SearchType searchType) {
        this.searchRequest.searchType(searchType);
    }

    /**
     * Get the search type to execute, defaults to {@link SearchType#DEFAULT}.
     */
    public SearchType getSearchType() {
        return this.searchRequest.searchType();
    }

    /**
     * Sets if this request should allow partial results. (If method is not called,
     * will default to the cluster level setting).
     */
    public void setAllowPartialSearchResults(boolean allowPartialSearchResults) {
        this.searchRequest.allowPartialSearchResults(allowPartialSearchResults);
    }

    /**
     * Gets if this request should allow partial results.
     */
    public Boolean getAllowPartialSearchResults() {
        return this.searchRequest.allowPartialSearchResults();
    }

    /**
     * Optional. Sets the number of shard results that should be reduced at once on the coordinating node.
     * This value should be used as a protection mechanism to reduce the memory overhead per search
     * request if the potential number of shards in the request can be large. Defaults to 5.
     */
    public void setBatchedReduceSize(int batchedReduceSize) {
        this.batchedReduceSize = batchedReduceSize;
    }

    /**
     * Gets the number of shard results that should be reduced at once on the coordinating node.
     * Returns {@code null} if unset.
     */
    public Integer getBatchedReduceSize() {
        return this.batchedReduceSize;
    }

    /**
     * Sets if this request should use the request cache or not, assuming that it can (for
     * example, if "now" is used, it will never be cached).
     * By default (if not set) this is turned on for {@link SubmitAsyncSearchRequest}.
     */
    public void setRequestCache(Boolean requestCache) {
        this.searchRequest.requestCache(requestCache);
    }

    /**
     * Gets if this request should use the request cache or not, if set.
     * This defaults to `true` on the server side if unset in the client.
     */
    public Boolean getRequestCache() {
        return this.searchRequest.requestCache();
    }

    /**
     * Returns the number of shard requests that should be executed concurrently on a single node.
     * The default is {@code 5}.
     */
    public int getMaxConcurrentShardRequests() {
        return this.searchRequest.getMaxConcurrentShardRequests();
    }

    /**
     * Sets the number of shard requests that should be executed concurrently on a single node.
     * The default is {@code 5}.
     */
    public void setMaxConcurrentShardRequests(int maxConcurrentShardRequests) {
        this.searchRequest.setMaxConcurrentShardRequests(maxConcurrentShardRequests);
    }

    /**
     * Gets if the source of the {@link SearchSourceBuilder} initially used on this request.
     */
    public SearchSourceBuilder getSearchSource() {
        return this.searchRequest.source();
    }

    @Override
    public Optional<ValidationException> validate() {
        final ValidationException validationException = new ValidationException();
        if (searchRequest.isSuggestOnly()) {
            validationException.addValidationError("suggest-only queries are not supported");
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
        return Objects.equals(searchRequest, request.searchRequest)
                && Objects.equals(getKeepAlive(), request.getKeepAlive())
                && Objects.equals(getWaitForCompletionTimeout(), request.getWaitForCompletionTimeout())
                && Objects.equals(isKeepOnCompletion(), request.isKeepOnCompletion());
    }

    @Override
    public int hashCode() {
        return Objects.hash(searchRequest, getKeepAlive(), getWaitForCompletionTimeout(), isKeepOnCompletion());
    }
}
