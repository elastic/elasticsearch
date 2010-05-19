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
import org.apache.lucene.search.TermRangeQuery;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.QueryParsingException;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.util.inject.Inject;
import org.elasticsearch.util.settings.Settings;
import org.elasticsearch.util.xcontent.XContentParser;

import java.io.IOException;

import static org.elasticsearch.index.query.support.QueryParsers.*;

/**
 * @author kimchy (shay.banon)
 */
public class RangeQueryParser extends AbstractIndexComponent implements XContentQueryParser {

    public static final String NAME = "range";

    @Inject public RangeQueryParser(Index index, @IndexSettings Settings settings) {
        super(index, settings);
    }

    @Override public String[] names() {
        return new String[]{NAME};
    }

    @Override public Query parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();

        XContentParser.Token token = parser.nextToken();
        assert token == XContentParser.Token.FIELD_NAME;
        String fieldName = parser.currentName();

        String from = null;
        String to = null;
        boolean includeLower = true;
        boolean includeUpper = true;
        float boost = 1.0f;

        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else {
                if ("from".equals(currentFieldName)) {
                    from = parser.textOrNull();
                } else if ("to".equals(currentFieldName)) {
                    to = parser.textOrNull();
                } else if ("include_lower".equals(currentFieldName) || "includeLower".equals(currentFieldName)) {
                    includeLower = parser.booleanValue();
                } else if ("include_upper".equals(currentFieldName) || "includeUpper".equals(currentFieldName)) {
                    includeUpper = parser.booleanValue();
                } else if ("boost".equals(currentFieldName)) {
                    boost = parser.floatValue();
                } else if ("gt".equals(currentFieldName)) {
                    from = parser.textOrNull();
                    includeLower = false;
                } else if ("gte".equals(currentFieldName) || "ge".equals(currentFieldName)) {
                    from = parser.textOrNull();
                    includeLower = true;
                } else if ("lt".equals(currentFieldName)) {
                    to = parser.textOrNull();
                    includeUpper = false;
                } else if ("lte".equals(currentFieldName) || "le".equals(currentFieldName)) {
                    to = parser.textOrNull();
                    includeUpper = true;
                }
            }
        }

        // move to the next end object, to close the field name
        token = parser.nextToken();
        assert token == XContentParser.Token.END_OBJECT;

        Query query = null;
        MapperService.SmartNameFieldMappers smartNameFieldMappers = parseContext.smartFieldMappers(fieldName);
        if (smartNameFieldMappers != null) {
            if (smartNameFieldMappers.hasMapper()) {
                query = smartNameFieldMappers.mapper().rangeQuery(from, to, includeLower, includeUpper);
            }
        }
        if (query == null) {
            query = new TermRangeQuery(fieldName, from, to, includeLower, includeUpper);
        }
        query.setBoost(boost);
        return wrapSmartNameQuery(query, smartNameFieldMappers, parseContext);
    }
}