/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

package org.elasticsearch.index.query.xcontent;

import org.apache.lucene.search.Query;
import org.apache.lucene.search.spans.SpanFirstQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.query.QueryParsingException;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.util.Strings;
import org.elasticsearch.util.inject.Inject;
import org.elasticsearch.util.settings.Settings;
import org.elasticsearch.util.xcontent.XContentParser;

import java.io.IOException;

/**
 * @author kimchy (shay.banon)
 */
public class SpanFirstQueryParser extends AbstractIndexComponent implements XContentQueryParser {

    public static final String NAME = "span_first";

    @Inject public SpanFirstQueryParser(Index index, @IndexSettings Settings settings) {
        super(index, settings);
    }

    @Override public String[] names() {
        return new String[]{NAME, Strings.toCamelCase(NAME)};
    }

    @Override public Query parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();

        float boost = 1.0f;

        SpanQuery match = null;
        int end = -1;

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("match".equals(currentFieldName)) {
                    Query query = parseContext.parseInnerQuery();
                    if (!(query instanceof SpanQuery)) {
                        throw new QueryParsingException(index, "spanFirst [match] must be of type span query");
                    }
                    match = (SpanQuery) query;
                }
            } else {
                if ("boost".equals(currentFieldName)) {
                    boost = parser.floatValue();
                } else if ("end".equals(currentFieldName)) {
                    end = parser.intValue();
                }
            }
        }
        if (match == null) {
            throw new QueryParsingException(index, "spanFirst must have [match] span query clause");
        }
        if (end == -1) {
            throw new QueryParsingException(index, "spanFirst must have [end] set for it");
        }

        SpanFirstQuery query = new SpanFirstQuery(match, end);
        query.setBoost(boost);
        return query;
    }
}