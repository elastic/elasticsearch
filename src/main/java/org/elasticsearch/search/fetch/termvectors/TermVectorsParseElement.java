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

import com.google.common.collect.Sets;
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.action.termvectors.TermVectorsRequest;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.internal.SearchContext;

import java.util.Set;

/**
 * Parses the {@code term_vectors} parameters in a search request.
 *
 * <pre>
 * {
 *   "query": {...},
 *   "term_vectors" : true|false|{term vectors parameters}
 * }
 * </pre>
 */public class TermVectorsParseElement implements SearchParseElement {

    private final static Set<String> disallowedParameters = Sets.newHashSet("_index", "_type", "_id", "doc", "_routing", "routing", 
            "_version", "version", "_version_type", "version_type", "_versionType", "versionType");

    @Override
    public void parse(XContentParser parser, SearchContext context) throws Exception {
        XContentParser.Token token = parser.currentToken();
        if (token == XContentParser.Token.VALUE_BOOLEAN) {
            if (parser.booleanValue()) {
                context.termVectorsContext(new TermVectorsContext());
            }
        } else if (token == XContentParser.Token.START_OBJECT) {
            TermVectorsRequest template = new TermVectorsRequest();
            TermVectorsRequest.parseRequest(template, parser, disallowedParameters);
            context.termVectorsContext(new TermVectorsContext(template));
        } else {
            throw new ElasticsearchIllegalStateException("Expected either a VALUE_BOOLEAN or a START_OBJECT but got " + token);
        }
    }
}
