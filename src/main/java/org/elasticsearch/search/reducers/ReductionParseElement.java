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

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.internal.SearchContext;

// NOCOMMIT put example request in pre block below
/**
 * The search parse element that is responsible for parsing the get part of the request.
 *
 * For example (in bold):
 * <pre>

 * </pre>
 */
public class ReductionParseElement implements SearchParseElement {

    private final ReducerParsers reducerParser;

    @Inject
    public ReductionParseElement(ReducerParsers reducerParser) {
        this.reducerParser = reducerParser;
    }

    @Override
    public void parse(XContentParser parser, SearchContext context) throws Exception {
        ReducerFactories factories = reducerParser.parseReducers(parser, context);
        context.reducers(new SearchContextReducers(factories));
    }
}
