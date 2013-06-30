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

package org.elasticsearch.index.query;

import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * A filter for a field based on several terms matching on any of them.
 */
public class TermsLookupFilterBuilder extends BaseFilterBuilder {

    private final String name;
    private String[] indices;
    private String[] types;
    private String id;
    private String routing;
    private String path;
    private Boolean lookupCache;
    private FilterBuilder lookupFilter;
    private Boolean cache;
    private String cacheKey;
    private String filterName;
    private Double bloomFpp;
    private Integer bloomExpectedInsertions;
    private Integer bloomHashFunctions;

    public TermsLookupFilterBuilder(String name) {
        this.name = name;
    }

    /**
     * Sets the filter name for the filter that can be used when searching for matched_filters per hit.
     */
    public TermsLookupFilterBuilder filterName(String filterName) {
        this.filterName = filterName;
        return this;
    }

    /**
     * Sets the index name to lookup the terms from.
     */
    public TermsLookupFilterBuilder index(String index) {
        this.indices = new String[]{index};
        return this;
    }

    /**
     * Sets the index name to lookup the terms from.
     */
    public TermsLookupFilterBuilder indices(String... indices) {
        this.indices = indices;
        return this;
    }

    /**
     * Sets the index type to lookup the terms from.
     */
    public TermsLookupFilterBuilder type(String type) {
        this.types = new String[]{type};
        return this;
    }

    /**
     * Sets the index type to lookup the terms from.
     */
    public TermsLookupFilterBuilder types(String... types) {
        this.types = types;
        return this;
    }

    /**
     * Sets the doc id to lookup the terms from.
     */
    public TermsLookupFilterBuilder id(String id) {
        this.id = id;
        return this;
    }

    /**
     * Sets the path within the document to lookup the terms from.
     */
    public TermsLookupFilterBuilder path(String path) {
        this.path = path;
        return this;
    }

    /**
     * Sets the filter used to lookup terms with
     */
    public TermsLookupFilterBuilder lookupFilter(FilterBuilder lookupFilter) {
        this.lookupFilter = lookupFilter;
        return this;
    }

    /**
     * Sets the node routing used to control the shards the lookup request is executed on
     */
    public TermsLookupFilterBuilder routing(String routing) {
        this.routing = routing;
        return this;
    }

    /**
     * Sets the BloomFilter false positive probability
     */
    public TermsLookupFilterBuilder bloomFpp(double bloomFpp) {
        this.bloomFpp = bloomFpp;
        return this;
    }

    /**
     * Sets the expected insertions to create the BloomFilter with
     */
    public TermsLookupFilterBuilder bloomExpectedInsertions(int bloomExpectedInsertions) {
        this.bloomExpectedInsertions = bloomExpectedInsertions;
        return this;
    }

    /**
     * Sets the number of hash functions used in the BloomFilter
     */
    public TermsLookupFilterBuilder bloomHashFunctions(int bloomHashFunctions) {
        this.bloomHashFunctions = bloomHashFunctions;
        return this;
    }

    /**
     * Sets if the gathered terms should be cached or not
     */
    public TermsLookupFilterBuilder lookupCache(boolean lookupCache) {
        this.lookupCache = lookupCache;
        return this;
    }

    /**
     * Sets if the resulting filter should be cached or not
     */
    public TermsLookupFilterBuilder cache(boolean cache) {
        this.cache = cache;
        return this;
    }

    /**
     * Sets the filter cache key
     */
    public TermsLookupFilterBuilder cacheKey(String cacheKey) {
        this.cacheKey = cacheKey;
        return this;
    }

    @Override
    public void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(TermsFilterParser.NAME);
        builder.startObject(name);
        if (indices != null) {
            builder.field("indices", indices);
        }

        if (types != null) {
            builder.field("types", types);
        }

        if (id != null) {
            builder.field("id", id);
        }

        if (routing != null) {
            builder.field("routing", routing);
        }

        if (lookupCache != null) {
            builder.field("cache", lookupCache);
        }

        if (lookupFilter != null) {
            builder.field("filter", lookupFilter);
        }

        if (path != null) {
            builder.field("path", path);
        }

        if (bloomFpp != null || bloomExpectedInsertions != null || bloomHashFunctions != null) {
            builder.startObject("bloom_filter");

            if (bloomFpp != null) {
                builder.field("fpp", bloomFpp);
            }

            if (bloomExpectedInsertions != null) {
                builder.field("expected_insertions", bloomExpectedInsertions);
            }

            if (bloomHashFunctions != null) {
                builder.field("hash_functions", bloomHashFunctions);
            }

            builder.endObject();
        }

        builder.endObject();

        if (filterName != null) {
            builder.field("_name", filterName);
        }

        if (cache != null) {
            builder.field("_cache", cache);
        }

        if (cacheKey != null) {
            builder.field("_cache_key", cacheKey);
        }
        builder.endObject();
    }
}
