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

package org.elasticsearch.search.rescore;

import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.ElasticSearchParseException;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.ParsedQuery;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.internal.SearchContext;

/**
 *
 */
public class RescoreParseElement implements SearchParseElement {

    @Override
    public void parse(XContentParser parser, SearchContext context) throws Exception {
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
                        throw new ElasticSearchParseException("rescore type malformed, must start with start_object");
                    }
                    rescoreContext = rescorer.parse(parser, context);
                }
            } else if (token.isValue()) {
                if ("window_size".equals(fieldName)) {
                    windowSize = parser.intValue();
                } else {
                    throw new ElasticSearchIllegalArgumentException("rescore doesn't support [" + fieldName + "]");
                }
            }
        }
        if (rescoreContext == null) {
            throw new ElasticSearchIllegalArgumentException("missing rescore type");
        }
        if (windowSize != null) {
            rescoreContext.setWindowSize(windowSize.intValue());
        }
        context.rescore(rescoreContext);
    }

}
