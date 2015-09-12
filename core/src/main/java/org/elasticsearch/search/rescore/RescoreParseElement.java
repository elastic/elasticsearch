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

package org.elasticsearch.search.rescore;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.internal.SearchContext;

/**
 *
 */
public class RescoreParseElement implements SearchParseElement {

    @Override
    public void parse(XContentParser parser, SearchContext context) throws Exception {
        if (parser.currentToken() == XContentParser.Token.START_ARRAY) {
            while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                parseSingleRescoreContext(parser, context);
            }
        } else {
            parseSingleRescoreContext(parser, context);
        }
    }

    public void parseSingleRescoreContext(XContentParser parser, SearchContext context) throws Exception {
        String fieldName = null;
        RescoreSearchContext rescoreContext = null;
        Integer windowSize = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
                if (QueryRescorer.NAME.equals(fieldName)) {
                    // we only have one at this point
                    Rescorer rescorer = QueryRescorer.INSTANCE;
                    token = parser.nextToken();
                    if (token != XContentParser.Token.START_OBJECT) {
                        throw new ElasticsearchParseException("rescore type malformed, must start with start_object");
                    }
                    rescoreContext = rescorer.parse(parser, context);
                }
            } else if (token.isValue()) {
                if ("window_size".equals(fieldName)) {
                    windowSize = parser.intValue();
                } else {
                    throw new IllegalArgumentException("rescore doesn't support [" + fieldName + "]");
                }
            }
        }
        if (rescoreContext == null) {
            throw new IllegalArgumentException("missing rescore type");
        }
        if (windowSize != null) {
            rescoreContext.setWindowSize(windowSize.intValue());
        }
        context.addRescore(rescoreContext);
    }

}
