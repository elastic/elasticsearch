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
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.support.QueryParsers;

import java.io.IOException;

/**
 * Implements the wildcard search query. Supported wildcards are <tt>*</tt>, which
 * matches any character sequence (including the empty one), and <tt>?</tt>,
 * which matches any single character. Note this query can be slow, as it
 * needs to iterate over many terms. In order to prevent extremely slow WildcardQueries,
 * a Wildcard term should not start with one of the wildcards <tt>*</tt> or
 * <tt>?</tt>.
 *
 *
 */
public class WildcardQueryBuilder extends BaseQueryBuilder implements QueryParser, MultiTermQueryBuilder, BoostableQueryBuilder<WildcardQueryBuilder> {

    private final String name;

    private final String wildcard;

    private float boost = -1;

    private String rewrite;

    private String queryName;
    
    
    public static final String NAME = "wildcard";

    @Inject
    public WildcardQueryBuilder() {
        this(null, null);
    }

    /**
     * Implements the wildcard search query. Supported wildcards are <tt>*</tt>, which
     * matches any character sequence (including the empty one), and <tt>?</tt>,
     * which matches any single character. Note this query can be slow, as it
     * needs to iterate over many terms. In order to prevent extremely slow WildcardQueries,
     * a Wildcard term should not start with one of the wildcards <tt>*</tt> or
     * <tt>?</tt>.
     *
     * @param name     The field name
     * @param wildcard The wildcard query string
     */
    public WildcardQueryBuilder(String name, String wildcard) {
        this.name = name;
        this.wildcard = wildcard;
    }

    public WildcardQueryBuilder rewrite(String rewrite) {
        this.rewrite = rewrite;
        return this;
    }

    /**
     * Sets the boost for this query.  Documents matching this query will (in addition to the normal
     * weightings) have their score multiplied by the boost provided.
     */
    @Override
    public WildcardQueryBuilder boost(float boost) {
        this.boost = boost;
        return this;
    }

    /**
     * Sets the query name for the filter that can be used when searching for matched_filters per hit.
     */
    public WildcardQueryBuilder queryName(String queryName) {
        this.queryName = queryName;
        return this;
    }

    @Override
    public void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(WildcardQueryBuilder.NAME);
        if (boost == -1 && rewrite == null && queryName != null) {
            builder.field(name, wildcard);
        } else {
            builder.startObject(name);
            builder.field("wildcard", wildcard);
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
            throw new QueryParsingException(parseContext.index(), "[wildcard] query malformed, no field");
        }
        String fieldName = parser.currentName();
        String rewriteMethod = null;

        String value = null;
        float boost = 1.0f;
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
                        rewriteMethod = parser.textOrNull();
                    } else if ("_name".equals(currentFieldName)) {
                        queryName = parser.text();
                    } else {
                        throw new QueryParsingException(parseContext.index(), "[wildcard] query does not support [" + currentFieldName + "]");
                    }
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

        BytesRef valueBytes;
        MapperService.SmartNameFieldMappers smartNameFieldMappers = parseContext.smartFieldMappers(fieldName);
        if (smartNameFieldMappers != null && smartNameFieldMappers.hasMapper()) {
            fieldName = smartNameFieldMappers.mapper().names().indexName();
            valueBytes = smartNameFieldMappers.mapper().indexedValueForSearch(value);
        } else {
            valueBytes = new BytesRef(value);
        }

        WildcardQuery wildcardQuery = new WildcardQuery(new Term(fieldName, valueBytes));
        QueryParsers.setRewriteMethod(wildcardQuery, rewriteMethod);
        wildcardQuery.setRewriteMethod(QueryParsers.parseRewriteMethod(rewriteMethod));
        wildcardQuery.setBoost(boost);
        if (queryName != null) {
            parseContext.addNamedQuery(queryName, wildcardQuery);
        }
        return wildcardQuery;
    }
}