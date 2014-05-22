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

package org.elasticsearch.action.benchmark;

import org.elasticsearch.action.benchmark.start.BenchmarkStartRequest;
import org.elasticsearch.action.benchmark.start.BenchmarkStartRequestBuilder;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.ElasticsearchIntegrationTest.between;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.randomFrom;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.randomBoolean;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.randomAsciiOfLengthBetween;

/**
 * Utilities for building randomized benchmark tests.
 */
public class BenchmarkTestUtil {

    public static final String BENCHMARK_NAME    = "test_benchmark";
    public static final String COMPETITOR_PREFIX = "competitor_";
    public static final String INDEX_PREFIX      = "test_index_";
    public static final String INDEX_TYPE        = "test_type";

    public static final int DEFAULT_LOW_INTERVAL_BOUND  = 1;
    public static final int DEFAULT_HIGH_INTERVAL_BOUND = 3;
    public static final int MEDIUM_LOW_INTERVAL_BOUND   = 3;
    public static final int MEDIUM_HIGH_INTERVAL_BOUND  = 7;

    public static final SearchType[] searchTypes = { SearchType.DFS_QUERY_THEN_FETCH,
                                                     SearchType.QUERY_THEN_FETCH,
                                                     SearchType.QUERY_AND_FETCH,
                                                     SearchType.DFS_QUERY_AND_FETCH,
                                                     SearchType.COUNT };

    public static enum TestIndexField {
        INT_FIELD("int_field"),
        FLOAT_FIELD("float_field"),
        BOOLEAN_FIELD("boolean_field"),
        STRING_FIELD("string_field");

        final String name;

        TestIndexField(String name) {
            this.name = name;
        }

        public String toString() {
            return name;
        }
    }

    public static enum TestQueryType {
        MATCH_ALL {
            @Override
            QueryBuilder getQuery() {
                return QueryBuilders.matchAllQuery();
            }
        },
        MATCH {
            @Override
            QueryBuilder getQuery() {
                return QueryBuilders.matchQuery(TestIndexField.STRING_FIELD.toString(),
                            randomAsciiOfLengthBetween(1, 3));
            }
        },
        TERM {
            @Override
            QueryBuilder getQuery() {
                return QueryBuilders.termQuery(TestIndexField.STRING_FIELD.toString(),
                            randomAsciiOfLengthBetween(1, 3));
            }
        },
        QUERY_STRING {
            @Override
            QueryBuilder getQuery() {
                return QueryBuilders.queryString(
                            randomAsciiOfLengthBetween(1, 3));
            }
        },
        WILDCARD {
            @Override
            QueryBuilder getQuery() {
                return QueryBuilders.wildcardQuery(
                            TestIndexField.STRING_FIELD.toString(), randomBoolean() ? "*" : "?");
            }
        };

        abstract QueryBuilder getQuery();
    }

    public static BenchmarkStartRequest randomRequest(Client client, String[] indices, int numExecutorNodes,
                                                      Map<String, BenchmarkSettings> competitionSettingsMap, SearchRequest... requests) {

        return randomRequest(client, indices, numExecutorNodes, competitionSettingsMap,
                DEFAULT_LOW_INTERVAL_BOUND, DEFAULT_HIGH_INTERVAL_BOUND, requests);
    }

    public static BenchmarkStartRequest randomRequest(Client client, String[] indices, int numExecutorNodes,
                                                      Map<String, BenchmarkSettings> competitionSettingsMap, String benchmarkName,
                                                      SearchRequest... requests) {

        final List<BenchmarkStartRequest> benchmarkRequests = randomRequests(1, client, indices,
                numExecutorNodes, competitionSettingsMap, DEFAULT_LOW_INTERVAL_BOUND, DEFAULT_HIGH_INTERVAL_BOUND, benchmarkName, requests);
        return benchmarkRequests.get(0);
    }

    public static List<BenchmarkStartRequest> randomRequests(int nRequests, Client client, String[] indices, int numExecutorNodes,
                                                      Map<String, BenchmarkSettings> competitionSettingsMap, SearchRequest... requests) {

        return randomRequests(nRequests, client, indices, numExecutorNodes,
                competitionSettingsMap, DEFAULT_LOW_INTERVAL_BOUND, DEFAULT_HIGH_INTERVAL_BOUND, BENCHMARK_NAME, requests);
    }

    public static BenchmarkStartRequest randomRequest(Client client, String[] indices, int numExecutorNodes,
                                                      Map<String, BenchmarkSettings> competitionSettingsMap,
                                                      int lowRandomIntervalBound, int highRandomIntervalBound,
                                                      SearchRequest... requests) {
        final List<BenchmarkStartRequest> benchmarkRequests = randomRequests(1, client, indices,
                numExecutorNodes, competitionSettingsMap, lowRandomIntervalBound, highRandomIntervalBound, BENCHMARK_NAME, requests);
        return benchmarkRequests.get(0);
    }

    public static List<BenchmarkStartRequest> randomRequests(int nRequests, Client client, String[] indices, int numExecutorNodes,
                                                             Map<String, BenchmarkSettings> competitionSettingsMap,
                                                             int lowRandomIntervalBound, int highRandomIntervalBound,
                                                             String namePrefix, SearchRequest... requests) {

        final String prefix = namePrefix == null ? BENCHMARK_NAME : namePrefix;

        final List<BenchmarkStartRequest> benchmarkRequests = new ArrayList<>();

        for (int i = 0; i < nRequests; i++) {

            final BenchmarkStartRequestBuilder builder = new BenchmarkStartRequestBuilder(client, indices);
            final BenchmarkSettings settings = randomSettings(lowRandomIntervalBound, highRandomIntervalBound);

            builder.setIterations(settings.iterations());
            builder.setConcurrency(settings.concurrency());
            builder.setMultiplier(settings.multiplier());
            builder.setSearchType(settings.searchType());
            builder.setWarmup(settings.warmup());
            builder.setNumExecutorNodes(numExecutorNodes);

            final int numCompetitors = between(lowRandomIntervalBound, highRandomIntervalBound);
            for (int j = 0; j < numCompetitors; j++) {
                builder.addCompetitor(randomCompetitor(client, COMPETITOR_PREFIX + j, indices,
                        competitionSettingsMap, lowRandomIntervalBound, highRandomIntervalBound, requests));
            }

            final BenchmarkStartRequest request = builder.request();
            request.benchmarkId(nRequests == 1 ? prefix : prefix + "_" + i);
            request.cascadeGlobalSettings();
            request.applyLateBoundSettings(indices, new String[]{INDEX_TYPE});

            benchmarkRequests.add(request);
        }
        return benchmarkRequests;
    }

    public static SearchRequest randomSearch(Client client, String[] indices) {

        final SearchRequestBuilder builder = new SearchRequestBuilder(client);
        builder.setIndices(indices);
        builder.setTypes(INDEX_TYPE);
        builder.setQuery(randomFrom(TestQueryType.values()).getQuery());
        return builder.request();
    }

    public static BenchmarkCompetitor randomCompetitor(Client client, String name, String[] indices,
                                                       Map<String, BenchmarkSettings> competitionSettingsMap,
                                                       int lowRandomIntervalBound, int highRandomIntervalBound, SearchRequest... requests) {

        final BenchmarkCompetitorBuilder builder = new BenchmarkCompetitorBuilder();
        final BenchmarkSettings settings = randomSettings(lowRandomIntervalBound, highRandomIntervalBound);

        builder.setClearCachesSettings(randomCacheSettings());
        builder.setIterations(settings.iterations());
        builder.setConcurrency(settings.concurrency());
        builder.setMultiplier(settings.multiplier());
        builder.setSearchType(settings.searchType());
        builder.setWarmup(settings.warmup());
        builder.setName(name);
        if (requests != null &&  requests.length != 0) {
            for (int i = 0; i < requests.length; i++) {
                builder.addSearchRequest(requests[i]);
                settings.addSearchRequest(requests[i]);
            }
        } else {
            final int numSearches = between(lowRandomIntervalBound, highRandomIntervalBound);
            for (int i = 0; i < numSearches; i++) {
                final SearchRequest searchRequest = randomSearch(client, indices);
                builder.addSearchRequest(searchRequest);
                settings.addSearchRequest(searchRequest);
            }
        }

        if (competitionSettingsMap != null) {
            competitionSettingsMap.put(name, settings);
        }

        return builder.build();
    }

    public static BenchmarkSettings.ClearCachesSettings randomCacheSettings() {

        final BenchmarkSettings.ClearCachesSettings settings = new BenchmarkSettings.ClearCachesSettings();

        settings.filterCache(randomBoolean());
        settings.fieldDataCache(randomBoolean());
        settings.idCache(randomBoolean());
        settings.recycler(randomBoolean());

        if (randomBoolean()) {
            final int numFieldsToClear = between(1, TestIndexField.values().length);
            final String[] fields = new String[numFieldsToClear];
            for (int i = 0; i < numFieldsToClear; i++) {
                fields[i] = TestIndexField.values()[i].toString();
            }
            settings.fields(fields);
        }

        return settings;
    }

    public static BenchmarkSettings randomSettings(int lowRandomIntervalBound, int highRandomIntervalBound) {

        final BenchmarkSettings settings = new BenchmarkSettings();

        settings.concurrency(between(lowRandomIntervalBound, highRandomIntervalBound), true);
        settings.iterations(between(lowRandomIntervalBound, highRandomIntervalBound), true);
        settings.multiplier(between(1, 50), true);
        settings.warmup(randomBoolean(), true);
        settings.searchType(searchTypes[between(0, searchTypes.length - 1)], true);

        return settings;
    }
}
