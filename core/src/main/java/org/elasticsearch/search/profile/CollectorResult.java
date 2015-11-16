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

package org.elasticsearch.search.profile;

import java.util.List;

/**
 * Public interface to access the profiled timings of the various
 * Collectors used in the search.  Children CollectorResult's may be
 * embedded inside of a parent CollectorResult
 */
public interface CollectorResult {

    public static final String REASON_SEARCH_COUNT = "search_count";
    public static final String REASON_SEARCH_TOP_HITS = "search_top_hits";
    public static final String REASON_SEARCH_TERMINATE_AFTER_COUNT = "search_terminate_after_count";
    public static final String REASON_SEARCH_POST_FILTER = "search_post_filter";
    public static final String REASON_SEARCH_MIN_SCORE = "search_min_score";
    public static final String REASON_SEARCH_MULTI = "search_multi";
    public static final String REASON_SEARCH_TIMEOUT = "search_timeout";
    public static final String REASON_AGGREGATION = "aggregation";
    public static final String REASON_AGGREGATION_GLOBAL = "aggregation_global";

    /**
     * Return the elapsed time for this Collector, inclusive of all children
     */
    long getTime();

    /**
     * Return the reason "hint" for the Collector, to provide a little more
     * context to the end user why the Collector was added to the query.
     */
    String getReason();

    /**
     * Return the Class name of this Collector
     */
    String getName();

    /**
     * Returns a list of children nested inside this Collector
     */
    List<CollectorResult> getProfiledChildren();
}
