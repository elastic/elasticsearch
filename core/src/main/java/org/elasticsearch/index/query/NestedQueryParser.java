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

import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.support.QueryInnerHits;

import java.io.IOException;

public class NestedQueryParser implements QueryParser<NestedQueryBuilder> {

    private static final NestedQueryBuilder PROTOTYPE = new NestedQueryBuilder("", EmptyQueryBuilder.PROTOTYPE);
    public static final ParseField SCORE_MODE_FIELD = new ParseField("score_mode");
    public static final ParseField PATH_FIELD = new ParseField("path");
    public static final ParseField QUERY_FIELD = new ParseField("query");
    public static final ParseField INNER_HITS_FIELD = new ParseField("inner_hits");

    @Override
    public String[] names() {
        return new String[]{NestedQueryBuilder.NAME, Strings.toCamelCase(NestedQueryBuilder.NAME)};
    }

    @Override
    public NestedQueryBuilder fromXContent(QueryParseContext parseContext) throws IOException {
        XContentParser parser = parseContext.parser();
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
        ScoreMode scoreMode = NestedQueryBuilder.DEFAULT_SCORE_MODE;
        String queryName = null;
        QueryBuilder query = null;
        String path = null;
        String currentFieldName = null;
        QueryInnerHits queryInnerHits = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (parseContext.parseFieldMatcher().match(currentFieldName, QUERY_FIELD)) {
                    query = parseContext.parseInnerQueryBuilder();
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, INNER_HITS_FIELD)) {
                    queryInnerHits = new QueryInnerHits(parser);
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[nested] query does not support [" + currentFieldName + "]");
                }
            } else if (token.isValue()) {
                if (parseContext.parseFieldMatcher().match(currentFieldName, PATH_FIELD)) {
                    path = parser.text();
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, AbstractQueryBuilder.BOOST_FIELD)) {
                    boost = parser.floatValue();
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, SCORE_MODE_FIELD)) {
                    String sScoreMode = parser.text();
                    if ("avg".equals(sScoreMode)) {
                        scoreMode = ScoreMode.Avg;
                    } else if ("min".equals(sScoreMode)) {
                        scoreMode = ScoreMode.Min;
                    } else if ("max".equals(sScoreMode)) {
                        scoreMode = ScoreMode.Max;
                    } else if ("total".equals(sScoreMode) || "sum".equals(sScoreMode)) {
                        scoreMode = ScoreMode.Total;
                    } else if ("none".equals(sScoreMode)) {
                        scoreMode = ScoreMode.None;
                    } else {
                        throw new ParsingException(parser.getTokenLocation(), "illegal score_mode for nested query [" + sScoreMode + "]");
                    }
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, AbstractQueryBuilder.NAME_FIELD)) {
                    queryName = parser.text();
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[nested] query does not support [" + currentFieldName + "]");
                }
            }
        }
        return new NestedQueryBuilder(path, query, scoreMode, queryInnerHits).queryName(queryName).boost(boost);
    }

    @Override
    public NestedQueryBuilder getBuilderPrototype() {
        return PROTOTYPE;
    }
}
