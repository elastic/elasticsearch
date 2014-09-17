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

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;

/**
 * Request builder for benchmarks
 */
public class BenchmarkRequestBuilder extends ActionRequestBuilder<BenchmarkRequest, BenchmarkResponse, BenchmarkRequestBuilder, Client> {

    public BenchmarkRequestBuilder(Client client, String[] indices) {
        super(client, new BenchmarkRequest(indices));
    }

    public BenchmarkRequestBuilder(Client client) {
        super(client, new BenchmarkRequest());
    }

    public BenchmarkRequestBuilder setAllowCacheClearing(boolean allowCacheClearing) {
        request.settings().allowCacheClearing(allowCacheClearing);
        return this;
    }

    public BenchmarkRequestBuilder setClearCachesSettings(BenchmarkSettings.ClearCachesSettings clearCachesSettings) {
        request.settings().clearCachesSettings(clearCachesSettings, false);
        return this;
    }

    public BenchmarkRequestBuilder addSearchRequest(SearchRequest... searchRequest) {
        request.settings().addSearchRequest(searchRequest);
        return this;
    }

    public BenchmarkRequestBuilder addCompetitor(BenchmarkCompetitor competitor) {
        request.addCompetitor(competitor);
        return this;
    }

    public BenchmarkRequestBuilder addCompetitor(BenchmarkCompetitorBuilder competitorBuilder) {
        return addCompetitor(competitorBuilder.build());
    }

    public BenchmarkRequestBuilder setNumExecutorNodes(int numExecutorNodes) {
        request.numExecutorNodes(numExecutorNodes);
        return this;
    }

    public BenchmarkRequestBuilder setIterations(int iterations) {
        request.settings().iterations(iterations, false);
        return this;
    }

    public BenchmarkRequestBuilder setConcurrency(int concurrency) {
        request.settings().concurrency(concurrency, false);
        return this;
    }

    public BenchmarkRequestBuilder setMultiplier(int multiplier) {
        request.settings().multiplier(multiplier, false);
        return this;
    }

    public BenchmarkRequestBuilder setNumSlowest(int numSlowest) {
        request.settings().numSlowest(numSlowest, false);
        return this;
    }

    public BenchmarkRequestBuilder setWarmup(boolean warmup) {
        request.settings().warmup(warmup, false);
        return this;
    }

    public BenchmarkRequestBuilder setBenchmarkId(String benchmarkId) {
        request.benchmarkName(benchmarkId);
        return this;
    }

    public BenchmarkRequestBuilder setSearchType(SearchType searchType) {
        request.settings().searchType(searchType, false);
        return this;
    }

    public BenchmarkRequestBuilder setVerbose(boolean verbose) {
        request.verbose(verbose);
        return this;
    }

    public BenchmarkRequestBuilder setPercentiles(double[] percentiles) {
        request.percentiles(percentiles);
        return this;
    }

    @Override
    protected void doExecute(ActionListener<BenchmarkResponse> listener) {
        client.bench(request, listener);
    }
}
