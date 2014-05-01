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

import org.elasticsearch.action.admin.indices.cache.clear.ClearIndicesCacheRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Settings that define how a benchmark should be executed.
 */
public class BenchmarkSettings implements Streamable {

    public static final int DEFAULT_CONCURRENCY = 5;
    public static final int DEFAULT_ITERATIONS = 5;
    public static final int DEFAULT_MULTIPLIER = 1000;
    public static final int DEFAULT_NUM_SLOWEST = 1;
    public static final boolean DEFAULT_WARMUP = true;
    public static final SearchType DEFAULT_SEARCH_TYPE = SearchType.DEFAULT;
    public static final double[] DEFAULT_PERCENTILES = { 10, 25, 50, 75, 90, 99 };

    private int concurrency = DEFAULT_CONCURRENCY;
    private int iterations = DEFAULT_ITERATIONS;
    private int multiplier = DEFAULT_MULTIPLIER;
    private int numSlowest = DEFAULT_NUM_SLOWEST;
    private boolean warmup = DEFAULT_WARMUP;
    private SearchType searchType = DEFAULT_SEARCH_TYPE;
    private ClearIndicesCacheRequest clearCaches = null;
    private ClearCachesSettings clearCachesSettings = null;
    private String[] indices = Strings.EMPTY_ARRAY;
    private String[] types = Strings.EMPTY_ARRAY;
    private List<SearchRequest> searchRequests = new ArrayList<>();

    private boolean concurrencyFinalized = false;
    private boolean iterationsFinalized = false;
    private boolean multiplierFinalized = false;
    private boolean numSlowestFinalized = false;
    private boolean warmupFinalized = false;
    private boolean searchTypeFinalized = false;
    private boolean clearCachesSettingsFinalized = false;
    private boolean allowCacheClearing = true;

    /**
     * List of indices to execute on
     * @return  Indices
     */
    public String[] indices() {
        return indices;
    }

    /**
     * Sets the indices to execute on
     * @param indices   Indices
     */
    public void indices(String[] indices) {
        this.indices = (indices == null) ? Strings.EMPTY_ARRAY : indices;
    }

    /**
     * List of types to execute on
     * @return  Types
     */
    public String[] types() {
        return types;
    }

    /**
     * Sets the types to execute on
     * @param types Types
     */
    public void types(String[] types) {
        this.types = (types == null) ? Strings.EMPTY_ARRAY : types;
    }

    /**
     * Gets the concurrency level
     * @return  Concurrency
     */
    public int concurrency() {
        return concurrency;
    }

    /**
     * Sets the concurrency level; determines how many threads will be executing the competition concurrently.
     * @param concurrency   Concurrency
     * @param finalize      If true, value cannot be overwritten by subsequent calls
     */
    public void concurrency(int concurrency, boolean finalize) {
        if (concurrencyFinalized) {
            return;
        }
        this.concurrency = concurrency;
        concurrencyFinalized = finalize;
    }

    /**
     * How many times to the competition.
     * @return  Iterations
     */
    public int iterations() {
        return iterations;
    }

    /**
     * How many times to run the competition. Requests will be executed a total of (iterations * multiplier).
     * @param iterations    Iterations
     * @param finalize      If true, value cannot be overwritten by subsequent calls
     */
    public void iterations(int iterations, boolean finalize) {
        if (iterationsFinalized) {
            return;
        }
        this.iterations = iterations;
        iterationsFinalized = finalize;
    }

    /**
     * Gets the multiplier
     * @return  Multiplier
     */
    public int multiplier() {
        return multiplier;
    }

    /**
     * Sets the multiplier. The multiplier determines how many times each iteration will be run.
     * @param multiplier    Multiplier
     * @param finalize      If true, value cannot be overwritten by subsequent calls
     */
    public void multiplier(int multiplier, boolean finalize) {
        if (multiplierFinalized) {
            return;
        }
        this.multiplier = multiplier;
        multiplierFinalized = finalize;
    }

    /**
     * Gets number of slow requests to track
     * @return  Number of slow requests to track
     */
    public int numSlowest() {
        return numSlowest;
    }

    /**
     * How many slow requests to track
     * @param numSlowest    Number of slow requests to track
     * @param finalize      If true, value cannot be overwritten by subsequent calls
     */
    public void numSlowest(int numSlowest, boolean finalize) {
        if (numSlowestFinalized) {
            return;
        }
        this.numSlowest = numSlowest;
        numSlowestFinalized = finalize;
    }

    /**
     * Whether to run a warmup search request
     * @return  True/false
     */
    public boolean warmup() {
        return warmup;
    }

    /**
     * Whether to run a warmup search request
     * @param warmup    True/false
     * @param finalize  If true, value cannot be overwritten by subsequent calls
     */
    public void warmup(boolean warmup, boolean finalize) {
        if (warmupFinalized) {
            return;
        }
        this.warmup = warmup;
        warmupFinalized = finalize;
    }

    /**
     * Gets the search type
     * @return  Search type
     */
    public SearchType searchType() {
        return searchType;
    }

    /**
     * Sets the search type
     * @param searchType    Search type
     * @param finalize      If true, value cannot be overwritten by subsequent calls
     */
    public void searchType(SearchType searchType, boolean finalize) {
        if (searchTypeFinalized) {
            return;
        }
        this.searchType = searchType;
        searchTypeFinalized = finalize;
    }

    /**
     * Gets the list of search requests to benchmark
     * @return  Search requests
     */
    public List<SearchRequest> searchRequests() {
        return searchRequests;
    }

    /**
     * Adds a search request to benchmark
     * @param searchRequest Search request
     */
    public void addSearchRequest(SearchRequest... searchRequest) {
        searchRequests.addAll(Arrays.asList(searchRequest));
    }

    /**
     * Gets the clear caches request
     * @return  The clear caches request
     */
    public ClearIndicesCacheRequest clearCaches() {
        return clearCaches;
    }

    public boolean allowCacheClearing() {
        return allowCacheClearing;
    }

    public void allowCacheClearing(boolean allowCacheClearing) {
        this.allowCacheClearing = allowCacheClearing;
    }

    /**
     * Gets the clear caches settings
     * @return  Clear caches settings
     */
    public ClearCachesSettings clearCachesSettings() {
        return clearCachesSettings;
    }

    /**
     * Sets the clear caches settings
     * @param clearCachesSettings   Clear caches settings
     * @param finalize              If true, value cannot be overwritten by subsequent calls
     */
    public void clearCachesSettings(ClearCachesSettings clearCachesSettings, boolean finalize) {
        if (clearCachesSettingsFinalized) {
            return;
        }
        this.clearCachesSettings = clearCachesSettings;
        clearCachesSettingsFinalized = finalize;
    }

    /**
     * Builds a clear cache request from the settings
     */
    public void buildClearCachesRequestFromSettings() {
        clearCaches = new ClearIndicesCacheRequest(indices);
        clearCaches.filterCache(clearCachesSettings.filterCache);
        clearCaches.fieldDataCache(clearCachesSettings.fieldDataCache);
        clearCaches.idCache(clearCachesSettings.idCache);
        clearCaches.recycler(clearCachesSettings.recyclerCache);
        clearCaches.fields(clearCachesSettings.fields);
        clearCaches.filterKeys(clearCachesSettings.filterKeys);
    }

    public void buildSearchRequestsFromSettings() {
        for (SearchRequest searchRequest : searchRequests) {
            searchRequest.indices(indices);
            searchRequest.types(types);
            searchRequest.searchType(searchType);
        }
    }

    /**
     * Merge another settings object with this one, ignoring fields which have been finalized.
     * This is a convenience so that a global settings object can cascade it's settings
     * down to specific competitors w/o inadvertently overwriting anything that has already been set.
     * @param otherSettings The settings to merge into this
     */
    public void merge(BenchmarkSettings otherSettings) {
        if (!concurrencyFinalized) {
            concurrency = otherSettings.concurrency;
        }
        if (!iterationsFinalized) {
            iterations = otherSettings.iterations;
        }
        if (!multiplierFinalized) {
            multiplier = otherSettings.multiplier;
        }
        if (!numSlowestFinalized) {
            numSlowest = otherSettings.numSlowest;
        }
        if (!warmupFinalized) {
            warmup = otherSettings.warmup;
        }
        if (!searchTypeFinalized) {
            searchType = otherSettings.searchType;
        }
        if (!clearCachesSettingsFinalized) {
            clearCachesSettings = otherSettings.clearCachesSettings;
        }
    }

    public static class ClearCachesSettings {
        private boolean filterCache = false;
        private boolean fieldDataCache = false;
        private boolean idCache = false;
        private boolean recyclerCache = false;
        private String[] fields = null;
        private String[] filterKeys = null;

        public void filterCache(boolean filterCache) {
            this.filterCache = filterCache;
        }
        public void fieldDataCache(boolean fieldDataCache) {
            this.fieldDataCache = fieldDataCache;
        }
        public void idCache(boolean idCache) {
            this.idCache = idCache;
        }
        public void recycler(boolean recyclerCache) {
            this.recyclerCache = recyclerCache;
        }
        public void fields(String[] fields) {
            this.fields = fields;
        }
        public void filterKeys(String[] filterKeys) {
            this.filterKeys = filterKeys;
        }
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        concurrency = in.readVInt();
        iterations = in.readVInt();
        multiplier = in.readVInt();
        numSlowest = in.readVInt();
        warmup = in.readBoolean();
        indices = in.readStringArray();
        types = in.readStringArray();
        searchType = SearchType.fromId(in.readByte());
        clearCaches = in.readOptionalStreamable(new ClearIndicesCacheRequest());
        final int size = in.readVInt();
        for (int i = 0; i < size; i++) {
            SearchRequest searchRequest = new SearchRequest();
            searchRequest.readFrom(in);
            searchRequests.add(searchRequest);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(concurrency);
        out.writeVInt(iterations);
        out.writeVInt(multiplier);
        out.writeVInt(numSlowest);
        out.writeBoolean(warmup);
        out.writeStringArray(indices);
        out.writeStringArray(types);
        out.writeByte(searchType.id());
        out.writeOptionalStreamable(clearCaches);
        out.writeVInt(searchRequests.size());
        for (SearchRequest searchRequest : searchRequests) {
            searchRequest.writeTo(out);
        }
    }
}
