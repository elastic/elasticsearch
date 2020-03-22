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

    public static final int DEFAULT_PRE_FILTER_SHARD_SIZE = 1;
    public static final int DEFAULT_BATCHED_REDUCE_SIZE = 5;
    private static final boolean DEFAULT_CCS_MINIMIZE_ROUNDTRIPS = false;
    private static final boolean DEFAULT_REQUEST_CACHE_VALUE = true;

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
        searchRequest.setCcsMinimizeRoundtrips(DEFAULT_CCS_MINIMIZE_ROUNDTRIPS);
        searchRequest.setPreFilterShardSize(DEFAULT_PRE_FILTER_SHARD_SIZE);
        searchRequest.setBatchedReduceSize(DEFAULT_BATCHED_REDUCE_SIZE);
        searchRequest.requestCache(DEFAULT_REQUEST_CACHE_VALUE);
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
     * Sets the number of shard results that should be reduced at once on the coordinating node. This value should be used as a protection
     * mechanism to reduce the memory overhead per search request if the potential number of shards in the request can be large.
     */
    public void setBatchedReduceSize(int batchedReduceSize) {
        this.searchRequest.setBatchedReduceSize(batchedReduceSize);
    }

    /**
     * Gets the number of shard results that should be reduced at once on the coordinating node.
     * This defaults to 5 for {@link SubmitAsyncSearchRequest}.
     */
    public int getBatchedReduceSize() {
        return this.searchRequest.getBatchedReduceSize();
    }

    /**
     * Sets if this request should use the request cache or not, assuming that it can (for
     * example, if "now" is used, it will never be cached). By default (not set, or null,
     * will default to the index level setting if request cache is enabled or not).
     */
    public void setRequestCache(Boolean requestCache) {
        this.searchRequest.requestCache(requestCache);
    }

    /**
     * Gets if this request should use the request cache or not.
     * Defaults to `true` for {@link SubmitAsyncSearchRequest}.
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
                && Objects.equals(getWaitForCompletion(), request.getWaitForCompletion())
                && Objects.equals(isCleanOnCompletion(), request.isCleanOnCompletion());
    }

    @Override
    public int hashCode() {
        return Objects.hash(searchRequest, getKeepAlive(), getWaitForCompletion(), isCleanOnCompletion());
    }
}
