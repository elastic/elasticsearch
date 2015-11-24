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
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

/**
 * Parser for exists query
 */
public class ExistsQueryParser implements QueryParser<ExistsQueryBuilder> {

    public static final ParseField FIELD_FIELD = new ParseField("field");

    @Override
    public String[] names() {
        return new String[]{ExistsQueryBuilder.NAME};
    }

    @Override
    public ExistsQueryBuilder fromXContent(QueryParseContext parseContext) throws IOException {
        XContentParser parser = parseContext.parser();

        String fieldPattern = null;
        String queryName = null;
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;

        XContentParser.Token token;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if (parseContext.parseFieldMatcher().match(currentFieldName, FIELD_FIELD)) {
                    fieldPattern = parser.text();
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, AbstractQueryBuilder.NAME_FIELD)) {
                    queryName = parser.text();
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, AbstractQueryBuilder.BOOST_FIELD)) {
                    boost = parser.floatValue();
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[" + ExistsQueryBuilder.NAME + "] query does not support [" + currentFieldName + "]");
                }
            } else {
                throw new ParsingException(parser.getTokenLocation(), "[" + ExistsQueryBuilder.NAME + "] unknown token [" + token + "] after [" + currentFieldName + "]");
            }
        }

        if (fieldPattern == null) {
            throw new ParsingException(parser.getTokenLocation(), "[" + ExistsQueryBuilder.NAME + "] must be provided with a [field]");
        }

        ExistsQueryBuilder builder = new ExistsQueryBuilder(fieldPattern);
        builder.queryName(queryName);
        builder.boost(boost);
        return builder;
    }

    @Override
    public ExistsQueryBuilder getBuilderPrototype() {
        return ExistsQueryBuilder.PROTOTYPE;
    }
}
