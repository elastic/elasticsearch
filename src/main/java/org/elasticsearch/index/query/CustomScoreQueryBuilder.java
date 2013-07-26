/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import com.google.common.collect.Maps;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;

/**
 * A query that uses a script to compute or influence the score of documents
 * that match with the inner query or filter.
 * 
 * @deprecated use {@link FunctionScoreQueryBuilder} instead.
 */
public class CustomScoreQueryBuilder extends BaseQueryBuilder implements BoostableQueryBuilder<CustomScoreQueryBuilder> {

    private final QueryBuilder queryBuilder;

    private final FilterBuilder filterBuilder;

    private String script;

    private String lang;

    private float boost = -1;

    private Map<String, Object> params = null;

    /**
     * Constructs a query that defines how the scores are computed or influenced
     * for documents that match with the specified query by a custom defined
     * script.
     * 
     * @param queryBuilder
     *            The query that defines what documents are custom scored by
     *            this query
     */
    public CustomScoreQueryBuilder(QueryBuilder queryBuilder) {
        this.queryBuilder = queryBuilder;
        this.filterBuilder = null;
    }

    /**
     * Constructs a query that defines how documents are scored that match with
     * the specified filter.
     * 
     * @param filterBuilder
     *            The filter that decides with documents are scored by this
     *            query.
     */
    public CustomScoreQueryBuilder(FilterBuilder filterBuilder) {
        this.filterBuilder = filterBuilder;
        this.queryBuilder = null;
    }

    /**
     * Sets the boost factor for this query.
     */
    public CustomScoreQueryBuilder script(String script) {
        this.script = script;
        return this;
    }

    /**
     * Sets the language of the script.
     */
    public CustomScoreQueryBuilder lang(String lang) {
        this.lang = lang;
        return this;
    }

    /**
     * Additional parameters that can be provided to the script.
     */
    public CustomScoreQueryBuilder params(Map<String, Object> params) {
        if (this.params == null) {
            this.params = params;
        } else {
            this.params.putAll(params);
        }
        return this;
    }

    /**
     * Additional parameters that can be provided to the script.
     */
    public CustomScoreQueryBuilder param(String key, Object value) {
        if (params == null) {
            params = Maps.newHashMap();
        }
        params.put(key, value);
        return this;
    }

    /**
     * Sets the boost for this query. Documents matching this query will (in
     * addition to the normal weightings) have their score multiplied by the
     * boost provided.
     */
    public CustomScoreQueryBuilder boost(float boost) {
        this.boost = boost;
        return this;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(CustomScoreQueryParser.NAME);
        if (queryBuilder != null) {
            builder.field("query");
            queryBuilder.toXContent(builder, params);
        } else if (filterBuilder != null) {
            builder.field("filter");
            filterBuilder.toXContent(builder, params);
        }
        builder.field("script", script);
        if (lang != null) {
            builder.field("lang", lang);
        }
        if (this.params != null) {
            builder.field("params", this.params);
        }
        if (boost != -1) {
            builder.field("boost", boost);
        }
        builder.endObject();
    }
}