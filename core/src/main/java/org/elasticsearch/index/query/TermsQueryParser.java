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

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.indices.cache.query.terms.TermsLookup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Parser for terms query and terms lookup.
 *
 * Filters documents that have fields that match any of the provided terms (not analyzed)
 *
 * It also supports a terms lookup mechanism which can be used to fetch the term values from
 * a document in an index.
 */
public class TermsQueryParser extends BaseQueryParser {

    private static final ParseField MIN_SHOULD_MATCH_FIELD = new ParseField("min_match", "min_should_match", "minimum_should_match")
            .withAllDeprecated("Use [bool] query instead");
    private static final ParseField DISABLE_COORD_FIELD = new ParseField("disable_coord").withAllDeprecated("Use [bool] query instead");
    private static final ParseField EXECUTION_FIELD = new ParseField("execution").withAllDeprecated("execution is deprecated and has no effect");

    @Inject
    public TermsQueryParser() {
    }

    @Override
    public String[] names() {
        return new String[]{TermsQueryBuilder.NAME, "in"};
    }

    @Override
    public QueryBuilder fromXContent(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();

        String fieldName = null;
        List<Object> values = null;
        String minShouldMatch = null;
        boolean disableCoord = TermsQueryBuilder.DEFAULT_DISABLE_COORD;
        TermsLookup termsLookup = null;

        String queryName = null;
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;

        XContentParser.Token token;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (parseContext.isDeprecatedSetting(currentFieldName)) {
                // skip
            } else if (token == XContentParser.Token.START_ARRAY) {
                if  (fieldName != null) {
                    throw new QueryParsingException(parseContext, "[terms] query does not support multiple fields");
                }
                fieldName = currentFieldName;
                values = parseValues(parseContext, parser);
            } else if (token == XContentParser.Token.START_OBJECT) {
                fieldName = currentFieldName;
                termsLookup = parseTermsLookup(parseContext, parser);
            } else if (token.isValue()) {
                if (parseContext.parseFieldMatcher().match(currentFieldName, EXECUTION_FIELD)) {
                    // ignore
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, MIN_SHOULD_MATCH_FIELD)) {
                    if (minShouldMatch != null) {
                        throw new IllegalArgumentException("[" + currentFieldName + "] is not allowed in a filter context for the [" + TermsQueryBuilder.NAME + "] query");
                    }
                    minShouldMatch = parser.textOrNull();
                } else if ("boost".equals(currentFieldName)) {
                    boost = parser.floatValue();
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, DISABLE_COORD_FIELD)) {
                    disableCoord = parser.booleanValue();
                } else if ("_name".equals(currentFieldName)) {
                    queryName = parser.text();
                } else {
                    throw new QueryParsingException(parseContext, "[terms] query does not support [" + currentFieldName + "]");
                }
            }
        }

        if (fieldName == null) {
            throw new QueryParsingException(parseContext, "terms query requires a field name, followed by array of terms or a document lookup specification");
        }
        TermsQueryBuilder termsQueryBuilder;
        if (values == null) {
            termsQueryBuilder = new TermsQueryBuilder(fieldName);
        } else {
            termsQueryBuilder = new TermsQueryBuilder(fieldName, values);
        }
        return termsQueryBuilder
                .disableCoord(disableCoord)
                .minimumShouldMatch(minShouldMatch)
                .termsLookup(termsLookup)
                .boost(boost)
                .queryName(queryName);
    }

    private static List<Object> parseValues(QueryParseContext parseContext, XContentParser parser) throws IOException {
        List<Object> values = new ArrayList<>();
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
            Object value = parser.objectBytes();
            if (value == null) {
                throw new QueryParsingException(parseContext, "No value specified for terms query");
            }
            values.add(value);
        }
        return values;
    }

    private static TermsLookup parseTermsLookup(QueryParseContext parseContext, XContentParser parser) throws IOException {
        TermsLookup termsLookup = new TermsLookup();
        XContentParser.Token token;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if ("index".equals(currentFieldName)) {
                    termsLookup.index(parser.textOrNull());
                } else if ("type".equals(currentFieldName)) {
                    termsLookup.type(parser.text());
                } else if ("id".equals(currentFieldName)) {
                    termsLookup.id(parser.text());
                } else if ("routing".equals(currentFieldName)) {
                    termsLookup.routing(parser.textOrNull());
                } else if ("path".equals(currentFieldName)) {
                    termsLookup.path(parser.text());
                } else {
                    throw new QueryParsingException(parseContext, "[terms] query does not support [" + currentFieldName
                            + "] within lookup element");
                }
            }
        }
        if (termsLookup.type() == null) {
            throw new QueryParsingException(parseContext, "[terms] query lookup element requires specifying the type");
        }
        if (termsLookup.id() == null) {
            throw new QueryParsingException(parseContext, "[terms] query lookup element requires specifying the id");
        }
        if (termsLookup.path() == null) {
            throw new QueryParsingException(parseContext, "[terms] query lookup element requires specifying the path");
        }
        return termsLookup;
    }

    @Override
    public TermsQueryBuilder getBuilderPrototype() {
        return TermsQueryBuilder.PROTOTYPE;
    }
}
