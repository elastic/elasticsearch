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

package org.elasticsearch.search.internal;

import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.Scroll;

import java.io.IOException;
import java.util.Map;

/**
 * Shard level request that represents a search.
 * It provides all the methods that the {@link org.elasticsearch.search.internal.SearchContext} needs.
 * Provides a cache key based on its content that can be used to cache shard level response.
 */
public interface ShardSearchRequest {

    String index();

    int shardId();

    String[] types();

    BytesReference source();

    void source(BytesReference source);

    BytesReference extraSource();

    int numberOfShards();

    SearchType searchType();

    String[] filteringAliases();

    long nowInMillis();

    String templateName();

    ScriptService.ScriptType templateType();

    Map<String, String> templateParams();

    BytesReference templateSource();

    Boolean queryCache();

    Scroll scroll();

    /**
     * This setting is internal and will be enabled when at least one node is on versions 1.0.x and 1.1.x to enable
     * scrolling that those versions support.
     *
     * @return Whether the scrolling should use regular search and incrementing the from on each round, which can
     * bring down nodes due to the big priority queues being generated to accommodate from + size hits for sorting.
     */
    boolean useSlowScroll();

    /**
     * Returns the cache key for this shard search request, based on its content
     */
    BytesReference cacheKey() throws IOException;
}
