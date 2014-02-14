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

package org.elasticsearch.common.lucene.search;

import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Filter;

/**
 * We still need sometimes to exclude deletes, because we don't remove them always with acceptDocs on filters
 */
public class XConstantScoreQuery extends ConstantScoreQuery {

    private final Filter actualFilter;

    public XConstantScoreQuery(Filter filter) {
        super(new ApplyAcceptedDocsFilter(filter));
        this.actualFilter = filter;
    }

    // trick so any external systems still think that its the actual filter we use, and not the
    // deleted filter
    @Override
    public Filter getFilter() {
        return this.actualFilter;
    }
}

