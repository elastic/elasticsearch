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

package org.elasticsearch.search;

import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContent;

/**
 * The hits of a search request.
 *
 *
 */
public interface SearchHits extends Streamable, ToXContent, Iterable<SearchHit> {

    /**
     * The total number of hits that matches the search request.
     */
    long totalHits();

    /**
     * The total number of hits that matches the search request.
     */
    long getTotalHits();

    /**
     * The maximum score of this query.
     */
    float maxScore();

    /**
     * The maximum score of this query.
     */
    float getMaxScore();

    /**
     * The hits of the search request (based on the search type, and from / size provided).
     */
    SearchHit[] hits();

    /**
     * Return the hit as the provided position.
     */
    SearchHit getAt(int position);

    /**
     * The hits of the search request (based on the search type, and from / size provided).
     */
    public SearchHit[] getHits();
}
