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

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.query.QueryParsingException;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.util.guice.inject.Inject;
import org.elasticsearch.util.settings.Settings;
import org.elasticsearch.util.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.util.collect.Lists.*;
import static org.elasticsearch.util.lucene.search.Queries.*;

/**
 * @author kimchy (shay.banon)
 */
public class BoolQueryParser extends AbstractIndexComponent implements XContentQueryParser {

    @Inject public BoolQueryParser(Index index, @IndexSettings Settings settings) {
        super(index, settings);
    }

    @Override public String[] names() {
        return new String[]{"bool"};
    }

    @Override public Query parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();

        boolean disableCoord = false;
        float boost = 1.0f;
        int minimumNumberShouldMatch = -1;

        List<BooleanClause> clauses = newArrayList();

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("must".equals(currentFieldName)) {
                    clauses.add(new BooleanClause(parseContext.parseInnerQuery(), BooleanClause.Occur.MUST));
                } else if ("must_not".equals(currentFieldName) || "mustNot".equals(currentFieldName)) {
                    clauses.add(new BooleanClause(parseContext.parseInnerQuery(), BooleanClause.Occur.MUST_NOT));
                } else if ("should".equals(currentFieldName)) {
                    clauses.add(new BooleanClause(parseContext.parseInnerQuery(), BooleanClause.Occur.SHOULD));
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if ("must".equals(currentFieldName)) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        clauses.add(new BooleanClause(parseContext.parseInnerQuery(), BooleanClause.Occur.MUST));
                    }
                } else if ("must_not".equals(currentFieldName) || "mustNot".equals(currentFieldName)) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        clauses.add(new BooleanClause(parseContext.parseInnerQuery(), BooleanClause.Occur.MUST_NOT));
                    }
                } else if ("should".equals(currentFieldName)) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        clauses.add(new BooleanClause(parseContext.parseInnerQuery(), BooleanClause.Occur.SHOULD));
                    }
                }
            } else if (token.isValue()) {
                if ("disable_coord".equals(currentFieldName) || "disableCoord".equals(currentFieldName)) {
                    disableCoord = parser.booleanValue();
                } else if ("minimum_number_should_match".equals(currentFieldName) || "minimumNumberShouldMatch".equals(currentFieldName)) {
                    minimumNumberShouldMatch = parser.intValue();
                } else if ("boost".equals(currentFieldName)) {
                    boost = parser.floatValue();
                }
            }
        }

        BooleanQuery query = new BooleanQuery(disableCoord);
        for (BooleanClause clause : clauses) {
            query.add(clause);
        }
        query.setBoost(boost);
        if (minimumNumberShouldMatch != -1) {
            query.setMinimumNumberShouldMatch(minimumNumberShouldMatch);
        }
        return optimizeQuery(fixNegativeQueryIfNeeded(query));
    }
}
