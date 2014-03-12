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
package org.elasticsearch.action.bench;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.bench.BenchmarkSettings.ClearCachesSettings;

/**
 * Builder for a benchmark competitor
 */
public class BenchmarkCompetitorBuilder {

    private final BenchmarkCompetitor competitor;

    /**
     * Constructs a new competitor builder to run a competition on the given indices
     * @param indices   Indices to run against
     */
    public BenchmarkCompetitorBuilder(String... indices) {
        competitor = new BenchmarkCompetitor(indices);
    }

    /**
     * If true, competition will run a 'warmup' round. This is to prevent timings from a cold start.
     * @param warmup    Whether to do a warmup
     * @return          this
     */
    public BenchmarkCompetitorBuilder setWarmup(boolean warmup) {
        competitor.settings().warmup(warmup, true);
        return this;
    }

    /**
     * Sets the list if indices to execute against
     * @param indices   Indices to run against
     * @return          this
     */
    public BenchmarkCompetitorBuilder setIndices(String... indices) {
        competitor.settings().indices(indices);
        return this;
    }

    /**
     * Sets the types of the indices to execute against
     * @param types     Types of indices
     * @return          this
     */
    public BenchmarkCompetitorBuilder setTypes(String... types) {
        competitor.settings().types(types);
        return this;
    }

    /**
     * Whether a competitor is allowed to clear index caches. If true, and if a
     * ClearCachesSettings has been set, the competitor will
     * submit an index cache clear action at the top of each iteration.
     * @param   allowCacheClearing  If true, allow caches to be cleared
     * @return  this
     */
    public BenchmarkCompetitorBuilder setAllowCacheClearing(boolean allowCacheClearing) {
        competitor.settings().allowCacheClearing(allowCacheClearing);
        return this;
    }

    /**
     * Describes how an index cache clear request should be executed.
     * @param clearCachesSettings   Description of how to clear caches
     * @return                      this
     */
    public BenchmarkCompetitorBuilder setClearCachesSettings(ClearCachesSettings clearCachesSettings) {
        competitor.settings().clearCachesSettings(clearCachesSettings, true);
        return this;
    }

    /**
     * Sets the concurrency level with which to run the competition. This determines the number of
     * actively executing searches which the competition will run in parallel.
     * @param concurrency   Number of searches to run concurrently
     * @return              this
     */
    public BenchmarkCompetitorBuilder setConcurrency(int concurrency) {
        competitor.settings().concurrency(concurrency, true);
        return this;
    }

    /**
     * Adds a search request to the competition
     * @param searchRequest     Search request
     * @return                  this
     */
    public BenchmarkCompetitorBuilder addSearchRequest(SearchRequest... searchRequest) {
        competitor.settings().addSearchRequest(searchRequest);
        return this;
    }

    /**
     * Sets the number of times to run each competition
     * @param iters     Number of times to run the competition
     * @return          this
     */
    public BenchmarkCompetitorBuilder setIterations(int iters) {
        competitor.settings().iterations(iters, true);
        return this;
    }

    /**
     * A 'multiplier' for each iteration. This specifies how many times to execute
     * the search requests within each iteration. The resulting number of total searches
     * executed will be: iterations X multiplier. Setting a higher multiplier will
     * smooth out results and dampen the effect of outliers.
     * @param multiplier    Iteration multiplier
     * @return              this
     */
    public BenchmarkCompetitorBuilder setMultiplier(int multiplier) {
        competitor.settings().multiplier(multiplier, true);
        return this;
    }

    /**
     * Sets the number of slowest requests to report
     * @param numSlowest    Number of slow requests to report
     * @return              this
     */
    public BenchmarkCompetitorBuilder setNumSlowest(int numSlowest) {
        competitor.settings().numSlowest(numSlowest, true);
        return this;
    }

    /**
     * Builds a competitor
     * @return  A new competitor
     */
    public BenchmarkCompetitor build() {
        return competitor;
    }

    /**
     * Sets the type of search to execute
     * @param searchType    Search type
     * @return              this
     */
    public BenchmarkCompetitorBuilder setSearchType(SearchType searchType) {
        competitor.settings().searchType(searchType, true);
        return this;
    }

    /**
     * Sets the user-supplied name to the competitor
     * @param name  Name
     * @return      this
     */
    public BenchmarkCompetitorBuilder setName(String name) {
        competitor.name(name);
        return this;
    }
}
