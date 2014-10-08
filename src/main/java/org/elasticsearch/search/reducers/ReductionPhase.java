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

package org.elasticsearch.search.reducers;

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.SearchPhase;
import org.elasticsearch.search.internal.SearchContext;

import java.util.Map;

public class ReductionPhase implements SearchPhase {
    
    private final ReductionParseElement parseElement;

    private final ReductionBinaryParseElement binaryParseElement;
    
    @Inject
    public ReductionPhase(ReductionParseElement parseElement, ReductionBinaryParseElement binaryParseElement) {
        this.parseElement = parseElement;
        this.binaryParseElement = binaryParseElement;
    }
    
    @Override
    public Map<String, ? extends SearchParseElement> parseElements() {
        return ImmutableMap.<String, SearchParseElement>builder()
                .put("reducers", parseElement)
                .put("reducers_binary", binaryParseElement)
                .put("reducersBinary", binaryParseElement)
                .build();
    }

    @Override
    public void preProcess(SearchContext context) {
    }

    @Override
    public void execute(SearchContext context) throws ElasticsearchException {
        if (context.reducers() == null) {
            context.queryResult().reducerFactories(null);
            return;
        }

        if (context.queryResult().reducerFactories() != null) {
            // no need to compute the reducers twice, they should be computed on a per context basis
            return;
        }

        ReducerFactories reducerFactories = context.reducers().factories();
        context.queryResult().reducerFactories(reducerFactories);

        // disable reducers so that they don't run on next pages in case of scrolling
        context.reducers(null);
    }

}
