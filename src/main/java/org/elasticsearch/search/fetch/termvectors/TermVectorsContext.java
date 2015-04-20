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

package org.elasticsearch.search.fetch.termvectors;

import org.elasticsearch.action.termvectors.TermVectorsRequest;
import org.elasticsearch.action.termvectors.TermVectorsResponse;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.termvectors.ShardTermVectorsService;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.internal.SearchContext;

/**
 *
 */
public class TermVectorsContext {
    
    private TermVectorsRequest termVectorsRequest;

    public TermVectorsContext() {
        this.termVectorsRequest = new TermVectorsRequest();
    }

    public TermVectorsContext(TermVectorsRequest termVectorsRequest) {
        this.termVectorsRequest = termVectorsRequest;
    }
    
    public TermVectorsResponse getResponse(SearchContext context, FetchSubPhase.HitContext hitContext) {
        ShardTermVectorsService termVectorsService = context.termVectorsService();
        TermVectorsRequest request = termVectorsRequest.index(
                context.shardTarget().index()).type(hitContext.hit().type()).id(hitContext.hit().getId());
        return termVectorsService.getTermVectors(request, context.shardTarget().index());
    }
}
