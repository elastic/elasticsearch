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

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

/**
 * Parser for missing query
 */
public class MissingQueryParser implements QueryParser<MissingQueryBuilder> {

    @Override
    public String[] names() {
        return new String[]{MissingQueryBuilder.NAME};
    }

    @Override
    public MissingQueryBuilder fromXContent(QueryParseContext parseContext) throws IOException {
        XContentParser parser = parseContext.parser();

        String fieldPattern = null;
        String queryName = null;
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
        boolean nullValue = MissingQueryBuilder.DEFAULT_NULL_VALUE;
        boolean existence = MissingQueryBuilder.DEFAULT_EXISTENCE_VALUE;

        XContentParser.Token token;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if ("field".equals(currentFieldName)) {
                    fieldPattern = parser.text();
                } else if ("null_value".equals(currentFieldName)) {
                    nullValue = parser.booleanValue();
                } else if ("existence".equals(currentFieldName)) {
                    existence = parser.booleanValue();
                } else if ("_name".equals(currentFieldName)) {
                    queryName = parser.text();
                } else if ("boost".equals(currentFieldName)) {
                    boost = parser.floatValue();
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[" + MissingQueryBuilder.NAME + "] query does not support [" + currentFieldName + "]");
                }
            } else {
                throw new ParsingException(parser.getTokenLocation(), "[" + MissingQueryBuilder.NAME + "] unknown token [" + token + "] after [" + currentFieldName + "]");
            }
        }

        if (fieldPattern == null) {
            throw new ParsingException(parser.getTokenLocation(), "missing must be provided with a [field]");
        }
        return new MissingQueryBuilder(fieldPattern, nullValue, existence)
                .boost(boost)
                .queryName(queryName);
    }

    @Override
    public MissingQueryBuilder getBuilderPrototype() {
        return MissingQueryBuilder.PROTOTYPE;
    }
}
