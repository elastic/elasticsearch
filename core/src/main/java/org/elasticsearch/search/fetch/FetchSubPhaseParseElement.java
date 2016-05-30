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

package org.elasticsearch.search.fetch;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.internal.SearchContext;

/**
 * A parse element for a {@link org.elasticsearch.search.fetch.FetchSubPhase} that is used when parsing a search request.
 */
public abstract class FetchSubPhaseParseElement<SubPhaseContext extends FetchSubPhaseContext> implements SearchParseElement {

    @Override
    final public void parse(XContentParser parser, SearchContext context) throws Exception {
        SubPhaseContext fetchSubPhaseContext = context.getFetchSubPhaseContext(getContextFactory());
        // this is to make sure that the SubFetchPhase knows it should execute
        fetchSubPhaseContext.setHitExecutionNeeded(true);
        innerParse(parser, fetchSubPhaseContext, context);
    }

    /**
     * Implement the actual parsing here.
     */
    protected abstract void innerParse(XContentParser parser, SubPhaseContext fetchSubPhaseContext, SearchContext searchContext) throws Exception;

    /**
     * Return the ContextFactory for this FetchSubPhase.
     */
    protected abstract FetchSubPhase.ContextFactory<SubPhaseContext> getContextFactory();
}
