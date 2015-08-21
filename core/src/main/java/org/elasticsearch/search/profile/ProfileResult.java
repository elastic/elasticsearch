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
 * Public interface used to access the profiled timing for a single query node
 * in the query tree
 */
public interface ProfileResult {

    /**
     * Retrieve the lucene description of this query (e.g. the "explain" text)
     */
    String getLuceneDescription();

    /**
     * Retrieve the name of the query (e.g. "TermQuery")
     */
    String getQueryName();

    /**
     * Returns the timing breakdown for this particular query node
     */
    ProfileBreakdown getTimeBreakdown();

    /**
     * Returns the total time (inclusive of children) for this query node.
     *
     * @return  elapsed time in nanoseconds
     */
    long getTime();

    /**
     * Returns the relative time (inclusive of children) for this query node.
     * This time is relative to all shards and is a good indicator of the
     * "cost" of a particular query on a single shard
     *
     * @return The relative elapsed time for this query
     */
    double getRelativeTime();

    /**
     * Returns a list of all profiled children queries
     */
    List<ProfileResult> getProfiledChildren();

    InternalProfileCollector getCollector();
}
