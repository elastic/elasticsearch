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

import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.MappedFieldType;

import java.io.IOException;

/**
 *
 */
public class TermQueryParser implements QueryParser {

    public static final String NAME = "term";

    private static final ParseField NAME_FIELD = new ParseField("_name").withAllDeprecated("query name is not supported in short version of term query");
    private static final ParseField BOOST_FIELD = new ParseField("boost").withAllDeprecated("boost is not supported in short version of term query");

    @Inject
    public TermQueryParser() {
    }

    @Override
    public String[] names() {
        return new String[]{NAME};
    }

    @Override
    public Query parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();

        String queryName = null;
        String fieldName = null;
        Object value = null;
        float boost = 1.0f;
        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (parseContext.isDeprecatedSetting(currentFieldName)) {
                // skip
            } else if (token == XContentParser.Token.START_OBJECT) {
                // also support a format of "term" : {"field_name" : { ... }}
                if (fieldName != null) {
                    throw new QueryParsingException(parseContext, "[term] query does not support different field names, use [bool] query instead");
                }
                fieldName = currentFieldName;
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                    } else {
                        if ("term".equals(currentFieldName)) {
                            value = parser.objectBytes();
                        } else if ("value".equals(currentFieldName)) {
                            value = parser.objectBytes();
                        } else if ("_name".equals(currentFieldName)) {
                            queryName = parser.text();
                        } else if ("boost".equals(currentFieldName)) {
                            boost = parser.floatValue();
                        } else {
                            throw new QueryParsingException(parseContext, "[term] query does not support [" + currentFieldName + "]");
                        }
                    }
                }
            } else if (token.isValue()) {
                if (parseContext.parseFieldMatcher().match(currentFieldName, NAME_FIELD)) {
                    queryName = parser.text();
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, BOOST_FIELD)) {
                    boost = parser.floatValue();
                } else {
                    if (fieldName != null) {
                        throw new QueryParsingException(parseContext, "[term] query does not support different field names, use [bool] query instead");
                    }
                    fieldName = currentFieldName;
                    value = parser.objectBytes();
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                throw new QueryParsingException(parseContext, "[term] query does not support array of values");
            }
        }

        if (value == null) {
            throw new QueryParsingException(parseContext, "No value specified for term query");
        }

        Query query = null;
        MappedFieldType fieldType = parseContext.fieldMapper(fieldName);
        if (fieldType != null) {
            query = fieldType.termQuery(value, parseContext);
        }
        if (query == null) {
            query = new TermQuery(new Term(fieldName, BytesRefs.toBytesRef(value)));
        }
        query.setBoost(boost);
        if (queryName != null) {
            parseContext.addNamedQuery(queryName, query);
        }
        return query;
    }
}
