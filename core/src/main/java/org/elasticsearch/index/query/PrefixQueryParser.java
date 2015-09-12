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
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.support.QueryParsers;

import java.io.IOException;

/**
 *
 */
public class PrefixQueryParser implements QueryParser {

    public static final String NAME = "prefix";

    private static final ParseField NAME_FIELD = new ParseField("_name").withAllDeprecated("query name is not supported in short version of prefix query");

    @Inject
    public PrefixQueryParser() {
    }

    @Override
    public String[] names() {
        return new String[]{NAME};
    }

    @Override
    public Query parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();

        String fieldName = parser.currentName();
        String rewriteMethod = null;
        String queryName = null;

        String value = null;
        float boost = 1.0f;
        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (parseContext.isDeprecatedSetting(currentFieldName)) {
                // skip
            } else if (token == XContentParser.Token.START_OBJECT) {
                fieldName = currentFieldName;
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                    } else {
                        if ("_name".equals(currentFieldName)) {
                            queryName = parser.text();
                        } else if ("value".equals(currentFieldName) || "prefix".equals(currentFieldName)) {
                            value = parser.textOrNull();
                        } else if ("boost".equals(currentFieldName)) {
                            boost = parser.floatValue();
                        } else if ("rewrite".equals(currentFieldName)) {
                            rewriteMethod = parser.textOrNull();
                        } else {
                            throw new QueryParsingException(parseContext, "[regexp] query does not support [" + currentFieldName + "]");
                        }
                    }
                }
            } else {
                if (parseContext.parseFieldMatcher().match(currentFieldName, NAME_FIELD)) {
                    queryName = parser.text();
                } else {
                    fieldName = currentFieldName;
                    value = parser.textOrNull();
                }
            }
        }

        if (value == null) {
            throw new QueryParsingException(parseContext, "No value specified for prefix query");
        }

        MultiTermQuery.RewriteMethod method = QueryParsers.parseRewriteMethod(parseContext.parseFieldMatcher(), rewriteMethod, null);

        Query query = null;
        MappedFieldType fieldType = parseContext.fieldMapper(fieldName);
        if (fieldType != null) {
            query = fieldType.prefixQuery(value, method, parseContext);
        }
        if (query == null) {
            PrefixQuery prefixQuery = new PrefixQuery(new Term(fieldName, BytesRefs.toBytesRef(value)));
            if (method != null) {
                prefixQuery.setRewriteMethod(method);
            }
            query = prefixQuery;
        }
        query.setBoost(boost);
        if (queryName != null) {
            parseContext.addNamedQuery(queryName, query);
        }
        return  query;
    }
}