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
 * Parser for wildcard query
 */
public class WildcardQueryParser implements QueryParser<WildcardQueryBuilder> {

    public static final ParseField WILDCARD_FIELD = new ParseField("wildcard");
    public static final ParseField VALUE_FIELD = new ParseField("value");
    public static final ParseField REWRITE_FIELD = new ParseField("rewrite");

    @Override
    public String[] names() {
        return new String[]{WildcardQueryBuilder.NAME};
    }

    @Override
    public WildcardQueryBuilder fromXContent(QueryParseContext parseContext) throws IOException {
        XContentParser parser = parseContext.parser();

        XContentParser.Token token = parser.nextToken();
        if (token != XContentParser.Token.FIELD_NAME) {
            throw new ParsingException(parser.getTokenLocation(), "[wildcard] query malformed, no field");
        }
        String fieldName = parser.currentName();
        String rewrite = null;

        String value = null;
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
        String queryName = null;
        token = parser.nextToken();
        if (token == XContentParser.Token.START_OBJECT) {
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else {
                    if (parseContext.parseFieldMatcher().match(currentFieldName, WILDCARD_FIELD)) {
                        value = parser.text();
                    } else if (parseContext.parseFieldMatcher().match(currentFieldName, VALUE_FIELD)) {
                        value = parser.text();
                    } else if (parseContext.parseFieldMatcher().match(currentFieldName, AbstractQueryBuilder.BOOST_FIELD)) {
                        boost = parser.floatValue();
                    } else if (parseContext.parseFieldMatcher().match(currentFieldName, REWRITE_FIELD)) {
                        rewrite = parser.textOrNull();
                    } else if (parseContext.parseFieldMatcher().match(currentFieldName, AbstractQueryBuilder.NAME_FIELD)) {
                        queryName = parser.text();
                    } else {
                        throw new ParsingException(parser.getTokenLocation(), "[wildcard] query does not support [" + currentFieldName + "]");
                    }
                }
            }
            parser.nextToken();
        } else {
            value = parser.text();
            parser.nextToken();
        }

        if (value == null) {
            throw new ParsingException(parser.getTokenLocation(), "No value specified for prefix query");
        }
        return new WildcardQueryBuilder(fieldName, value)
                .rewrite(rewrite)
                .boost(boost)
                .queryName(queryName);
    }

    @Override
    public WildcardQueryBuilder getBuilderPrototype() {
        return WildcardQueryBuilder.PROTOTYPE;
    }
}
