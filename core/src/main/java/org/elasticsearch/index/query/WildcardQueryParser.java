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
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

/**
 * Parser for wildcard query
 */
public class WildcardQueryParser extends BaseQueryParser<WildcardQueryBuilder> {

    @Override
    public String[] names() {
        return new String[]{WildcardQueryBuilder.NAME};
    }

    @Override
    public WildcardQueryBuilder fromXContent(QueryParseContext parseContext) throws IOException {
        XContentParser parser = parseContext.parser();

        XContentParser.Token token = parser.nextToken();
        if (token != XContentParser.Token.FIELD_NAME) {
            throw new ParsingException(parseContext, "[wildcard] query malformed, no field");
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
                    if ("wildcard".equals(currentFieldName)) {
                        value = parser.text();
                    } else if ("value".equals(currentFieldName)) {
                        value = parser.text();
                    } else if ("boost".equals(currentFieldName)) {
                        boost = parser.floatValue();
                    } else if ("rewrite".equals(currentFieldName)) {
                        rewrite = parser.textOrNull();
                    } else if ("_name".equals(currentFieldName)) {
                        queryName = parser.text();
                    } else {
                        throw new ParsingException(parseContext, "[wildcard] query does not support [" + currentFieldName + "]");
                    }
                }
            }
            parser.nextToken();
        } else {
            value = parser.text();
            parser.nextToken();
        }

        if (value == null) {
            throw new ParsingException(parseContext, "No value specified for prefix query");
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
