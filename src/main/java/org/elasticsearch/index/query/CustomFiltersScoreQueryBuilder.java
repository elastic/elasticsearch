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

import com.carrotsearch.hppc.FloatArrayList;
import com.google.common.collect.Maps;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

/**
 * A query that uses a filters with a script associated with them to compute the
 * score.
 * 
 * @deprecated use {@link FunctionScoreQueryBuilder} instead.
 */
public class CustomFiltersScoreQueryBuilder extends BaseQueryBuilder implements BoostableQueryBuilder<CustomFiltersScoreQueryBuilder> {

    private final QueryBuilder queryBuilder;

    private String lang;

    private float boost = -1;

    private Float maxBoost;

    private Map<String, Object> params = null;

    private String scoreMode;

    private ArrayList<FilterBuilder> filters = new ArrayList<FilterBuilder>();
    private ArrayList<String> scripts = new ArrayList<String>();
    private FloatArrayList boosts = new FloatArrayList();

    public CustomFiltersScoreQueryBuilder(QueryBuilder queryBuilder) {
        this.queryBuilder = queryBuilder;
    }

    public CustomFiltersScoreQueryBuilder add(FilterBuilder filter, String script) {
        this.filters.add(filter);
        this.scripts.add(script);
        this.boosts.add(-1);
        return this;
    }

    public CustomFiltersScoreQueryBuilder add(FilterBuilder filter, float boost) {
        this.filters.add(filter);
        this.scripts.add(null);
        this.boosts.add(boost);
        return this;
    }

    public CustomFiltersScoreQueryBuilder scoreMode(String scoreMode) {
        this.scoreMode = scoreMode;
        return this;
    }

    /**
     * Sets the language of the script.
     */
    public CustomFiltersScoreQueryBuilder lang(String lang) {
        this.lang = lang;
        return this;
    }

    /**
     * Additional parameters that can be provided to the script.
     */
    public CustomFiltersScoreQueryBuilder params(Map<String, Object> params) {
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
    public CustomFiltersScoreQueryBuilder param(String key, Object value) {
        if (params == null) {
            params = Maps.newHashMap();
        }
        params.put(key, value);
        return this;
    }

    public CustomFiltersScoreQueryBuilder maxBoost(float maxBoost) {
        this.maxBoost = maxBoost;
        return this;
    }

    /**
     * Sets the boost for this query. Documents matching this query will (in
     * addition to the normal weightings) have their score multiplied by the
     * boost provided.
     */
    public CustomFiltersScoreQueryBuilder boost(float boost) {
        this.boost = boost;
        return this;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(CustomFiltersScoreQueryParser.NAME);
        builder.field("query");
        queryBuilder.toXContent(builder, params);

        builder.startArray("filters");
        for (int i = 0; i < filters.size(); i++) {
            builder.startObject();
            builder.field("filter");
            filters.get(i).toXContent(builder, params);
            String script = scripts.get(i);
            if (script != null) {
                builder.field("script", script);
            } else {
                builder.field("boost", boosts.get(i));
            }
            builder.endObject();
        }
        builder.endArray();

        if (scoreMode != null) {
            builder.field("score_mode", scoreMode);
        }
        if (maxBoost != null) {
            builder.field("max_boost", maxBoost);
        }

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