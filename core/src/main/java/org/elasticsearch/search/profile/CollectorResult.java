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

    /**
     * Collectors are used for a variety of purposes (scoring, limiting time, etc).  To help the
     * user understand what the collector does, an optional CollectorReason can be supplied
     * when instantiating the CollectorResult
     */
    enum CollectorReason {
        GENERAL(0), SEARCH_COUNT(1), SEARCH_SCAN(2), SEARCH_SORTED(3), SEARCH_TERMINATE_AFTER_COUNT(4),
        SEARCH_POST_FILTER(5), SEARCH_MIN_SCORE(6), SEARCH_MULTI(7), SEARCH_TIMEOUT(8), AGGREGATION(9), AGGREGATION_GLOBAL(10);

        private int reason;

        CollectorReason(int reason) {
            this.reason = reason;
        }

        public int getReason() {
            return reason;
        }

        @Override
        public String toString() {
            return name().toLowerCase();
        }

        public static CollectorReason fromInt(int reason) {
            CollectorReason[] values = CollectorReason.values();
            for (CollectorReason value : values) {
                if (value.getReason() == reason) {
                    return value;
                }
            }
            return GENERAL;
        }
    }

    /**
     * Return the elapsed time for this Collector, inclusive of all children
     */
    long getTime();

    /**
     * Return the relative time (inclusive of children) of this Collector.
     * This time is relative to all shards and is a good indicator of the
     * "cost" of a Collector and it's children
     */
    double getRelativeTime();

    /**
     * Return the reason "hint" for the Collector, to provide a little more
     * context to the end user why the Collector was added to the query.
     */
    CollectorReason getReason();

    /**
     * Return the Class name of this Collector
     */
    String getName();

    /**
     * Returns a list of children nested inside this Collector
     */
    List<CollectorResult> getProfiledChildren();
}
