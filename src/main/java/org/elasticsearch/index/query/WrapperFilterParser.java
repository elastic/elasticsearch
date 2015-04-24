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

package org.elasticsearch.index.query;

import org.apache.lucene.search.Filter;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

/**
 * Filter parser for embedded filter.
 */
public class WrapperFilterParser implements FilterParser {

    public static final String NAME = "wrapper";

    @Inject
    public WrapperFilterParser() {
    }

    @Override
    public String[] names() {
        return new String[]{NAME};
    }

    @Override
    public Filter parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();

        XContentParser.Token token = parser.nextToken();
        if (token != XContentParser.Token.FIELD_NAME) {
            throw new QueryParsingException(parseContext.index(), "[wrapper] filter malformed");
        }
        String fieldName = parser.currentName();
        if (!fieldName.equals("filter")) {
            throw new QueryParsingException(parseContext.index(), "[wrapper] filter malformed");
        }
        parser.nextToken();

        byte[] querySource = parser.binaryValue();
        try (XContentParser qSourceParser = XContentFactory.xContent(querySource).createParser(querySource)) {
            final QueryParseContext context = new QueryParseContext(parseContext.index(), parseContext.indexQueryParserService());
            context.reset(qSourceParser);
            Filter result = context.parseInnerFilter();
            parser.nextToken();
            parseContext.combineNamedFilters(context);
            return result;
        }
    }
}
