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

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

/**
 * Parser for limit query
 * @deprecated use terminate_after feature instead
 */
@Deprecated
public class LimitQueryParser extends BaseQueryParser<LimitQueryBuilder> {

    @Inject
    public LimitQueryParser() {
    }

    @Override
    public String[] names() {
        return new String[]{LimitQueryBuilder.NAME};
    }

    @Override
    public LimitQueryBuilder fromXContent(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();

        int limit = -1;
        String queryName = null;
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if ("value".equals(currentFieldName)) {
                    limit = parser.intValue();
                } else if ("_name".equals(currentFieldName)) {
                    queryName = parser.text();
                } else if ("boost".equals(currentFieldName)) {
                    boost = parser.floatValue();
                } else {
                    throw new QueryParsingException(parseContext, "[limit] query does not support [" + currentFieldName + "]");
                }
            }
        }

        if (limit == -1) {
            throw new QueryParsingException(parseContext, "No value specified for limit query");
        }

        return new LimitQueryBuilder(limit).boost(boost).queryName(queryName);
    }

    @Override
    public LimitQueryBuilder getBuilderPrototype() {
        return LimitQueryBuilder.PROTOTYPE;
    }
}
