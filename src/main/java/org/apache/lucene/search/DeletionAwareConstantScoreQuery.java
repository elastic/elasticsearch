/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.apache.lucene.search;

import org.elasticsearch.common.lucene.search.NotDeletedFilter;

/**
 *
 */
// LUCENE MONITOR: Against ConstantScoreQuery, basically added logic in the doc iterator to take deletions into account
// So it can basically be cached safely even with a reader that changes deletions but remain with teh same cache key
// See more: https://issues.apache.org/jira/browse/LUCENE-2468
// TODO Lucene 4.0 won't need this, since live docs are "and'ed" while scoring
public class DeletionAwareConstantScoreQuery extends ConstantScoreQuery {

    private final Filter actualFilter;

    public DeletionAwareConstantScoreQuery(Filter filter) {
        super(new NotDeletedFilter(filter));
        this.actualFilter = filter;
    }

    // trick so any external systems still think that its the actual filter we use, and not the
    // deleted filter
    @Override
    public Filter getFilter() {
        return this.actualFilter;
    }
}

