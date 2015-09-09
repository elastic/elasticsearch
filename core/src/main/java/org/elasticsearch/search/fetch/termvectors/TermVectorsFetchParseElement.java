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

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.fetch.FetchSubPhaseParseElement;
import org.elasticsearch.search.internal.SearchContext;

import java.util.ArrayList;
import java.util.List;

public class TermVectorsFetchParseElement extends FetchSubPhaseParseElement<TermVectorsFetchContext> {

    @Override
    protected void innerParse(XContentParser parser, TermVectorsFetchContext termVectorsFetchContext, SearchContext searchContext) throws Exception {
        List<String> fields = new ArrayList<>();
        XContentParser.Token token = parser.currentToken();
        if (token == XContentParser.Token.START_ARRAY) {
            while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                fields.add(parser.text());
            }
        } else {
            throw new IllegalStateException("Expected a START_ARRAY but got " + token);
        }
        termVectorsFetchContext.setFields(fields.toArray(new String[0]));
    }

    @Override
    protected FetchSubPhase.ContextFactory getContextFactory() {
        return TermVectorsFetchSubPhase.CONTEXT_FACTORY;
    }
}
