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

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

/**
 * Parser for type query
 */
public class TypeQueryParser implements QueryParser<TypeQueryBuilder> {

    public static final ParseField VALUE_FIELD = new ParseField("value");

    @Override
    public String[] names() {
        return new String[]{TypeQueryBuilder.NAME};
    }

    @Override
    public TypeQueryBuilder fromXContent(QueryParseContext parseContext) throws IOException {
        XContentParser parser = parseContext.parser();
        BytesRef type = null;

        String queryName = null;
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if (parseContext.parseFieldMatcher().match(currentFieldName, AbstractQueryBuilder.NAME_FIELD)) {
                    queryName = parser.text();
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, AbstractQueryBuilder.BOOST_FIELD)) {
                    boost = parser.floatValue();
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, VALUE_FIELD)) {
                    type = parser.utf8Bytes();
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[" + TypeQueryBuilder.NAME + "] filter doesn't support [" + currentFieldName + "]");
                }
            } else {
                throw new ParsingException(parser.getTokenLocation(), "[" + TypeQueryBuilder.NAME + "] filter doesn't support [" + currentFieldName + "]");
            }
        }

        if (type == null) {
            throw new ParsingException(parser.getTokenLocation(), "[" + TypeQueryBuilder.NAME + "] filter needs to be provided with a value for the type");
        }
        return new TypeQueryBuilder(type)
                .boost(boost)
                .queryName(queryName);
    }

    @Override
    public TypeQueryBuilder getBuilderPrototype() {
        return TypeQueryBuilder.PROTOTYPE;
    }
}