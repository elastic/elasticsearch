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
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.support.QueryParsers;

import java.io.IOException;

/**
 * A Query that matches documents containing terms with a specified prefix.
 *
 *
 */
public class PrefixQueryBuilder extends BaseQueryBuilder implements QueryParser, MultiTermQueryBuilder, BoostableQueryBuilder<PrefixQueryBuilder> {

    public static final String NAME = "prefix";

    private final String name;

    private final String prefix;

    private float boost = -1;

    private String rewrite;

    private String queryName;

    /**
     * A Query that matches documents containing terms with a specified prefix.
     *
     * @param name   The name of the field
     * @param prefix The prefix query
     */
    public PrefixQueryBuilder(String name, String prefix) {
        this.name = name;
        this.prefix = prefix;
    }

    @Inject
    public PrefixQueryBuilder() {
        this.name = null;
        this.prefix = null;
    }

    /**
     * Sets the boost for this query.  Documents matching this query will (in addition to the normal
     * weightings) have their score multiplied by the boost provided.
     */
    @Override
    public PrefixQueryBuilder boost(float boost) {
        this.boost = boost;
        return this;
    }

    public PrefixQueryBuilder rewrite(String rewrite) {
        this.rewrite = rewrite;
        return this;
    }

    /**
     * Sets the query name for the filter that can be used when searching for matched_filters per hit.
     */
    public PrefixQueryBuilder queryName(String queryName) {
        this.queryName = queryName;
        return this;
    }

    @Override
    public void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(PrefixQueryBuilder.NAME);
        if (boost == -1 && rewrite == null && queryName != null) {
            builder.field(name, prefix);
        } else {
            builder.startObject(name);
            builder.field("prefix", prefix);
            if (boost != -1) {
                builder.field("boost", boost);
            }
            if (rewrite != null) {
                builder.field("rewrite", rewrite);
            }
            if (queryName != null) {
                builder.field("_name", queryName);
            }
            builder.endObject();
        }
        builder.endObject();
    }

    @Override
    public String[] names() {
        return new String[]{NAME};
    }

    @Override
    public Query parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();

        XContentParser.Token token = parser.nextToken();
        if (token != XContentParser.Token.FIELD_NAME) {
            throw new QueryParsingException(parseContext.index(), "[prefix] query malformed, no field");
        }
        String fieldName = parser.currentName();
        String rewriteMethod = null;
        String queryName = null;

        Object value = null;
        float boost = 1.0f;
        token = parser.nextToken();
        if (token == XContentParser.Token.START_OBJECT) {
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token.isValue()) {
                    if ("prefix".equals(currentFieldName)) {
                        value = parser.objectBytes();
                    } else if ("value".equals(currentFieldName)) {
                        value = parser.text();
                    } else if ("boost".equals(currentFieldName)) {
                        boost = parser.floatValue();
                    } else if ("rewrite".equals(currentFieldName)) {
                        rewriteMethod = parser.textOrNull();
                    } else if ("_name".equals(currentFieldName)) {
                        queryName = parser.text();
                    }
                } else {
                    throw new QueryParsingException(parseContext.index(), "[prefix] query does not support [" + currentFieldName + "]");
                }
            }
            parser.nextToken();
        } else {
            value = parser.text();
            parser.nextToken();
        }

        if (value == null) {
            throw new QueryParsingException(parseContext.index(), "No value specified for prefix query");
        }

        MultiTermQuery.RewriteMethod method = QueryParsers.parseRewriteMethod(rewriteMethod, null);

        Query query = null;
        MapperService.SmartNameFieldMappers smartNameFieldMappers = parseContext.smartFieldMappers(fieldName);
        if (smartNameFieldMappers != null && smartNameFieldMappers.hasMapper()) {
            query = smartNameFieldMappers.mapper().prefixQuery(value, method, parseContext);
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
