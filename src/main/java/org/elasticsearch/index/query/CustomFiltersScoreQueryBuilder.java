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
import gnu.trove.list.array.TFloatArrayList;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

/**
 * A query that uses a filters with a script associated with them to compute the score.
 */
public class CustomFiltersScoreQueryBuilder extends BaseQueryBuilder {

    private final QueryBuilder queryBuilder;

    private String lang;

    private float boost = -1;

    private Map<String, Object> params = null;

    private CustomFiltersScoreGroup filters = new CustomFiltersScoreGroup();

    public CustomFiltersScoreQueryBuilder(QueryBuilder queryBuilder) {
        this.queryBuilder = queryBuilder;
    }

    public CustomFiltersScoreQueryBuilder add(FilterBuilder filter, String script) {
        this.filters.add(filter, script);
        return this;
    }

    public CustomFiltersScoreQueryBuilder add(FilterBuilder filter, float boost) {
        this.filters.add(filter, boost);
        return this;
    }

    public CustomFiltersScoreQueryBuilder scoreMode(String scoreMode) {
        this.filters.scoreMode(scoreMode);
        return this;
    }

    public CustomFiltersScoreQueryBuilder add(CustomFiltersScoreGroup queryBuilder) {
        filters.add(queryBuilder);
        return this;
    }

    public CustomFiltersScoreQueryBuilder set(CustomFiltersScoreGroup queryBuilder) {
        filters = queryBuilder;
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

    /**
     * Sets the boost for this query.  Documents matching this query will (in addition to the normal
     * weightings) have their score multiplied by the boost provided.
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

        // COMMIT COMMENT: 26-Dec-2011 === create a query that matches the old style (previous version) if nested score_mode groups are not used

        builder.startArray("filters");
        this.filters.doXContent(builder, params, false);
        builder.endArray();

        if (this.filters.scoreMode != null) {
            builder.field("score_mode", this.filters.scoreMode);
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

    protected static class CustomFiltersScoreFilter {
        FilterBuilder filter;
        String script;
        float boost;

        public CustomFiltersScoreFilter(FilterBuilder filter, String script) {
            this.filter = filter;
            this.script = script;
            this.boost = -1;
        }

        public CustomFiltersScoreFilter(FilterBuilder filter, float boost) {
            this.filter = filter;
            this.script = null;
            this.boost = boost;
        }

        protected void doXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("filter");
            filter.toXContent(builder, params);
            if (script != null) {
                builder.field("script", script);
            } else {
                builder.field("boost", boost);
            }
            builder.endObject();
        }
    }


    public static class CustomFiltersScoreGroup {
        private String scoreMode;
        private ArrayList<Object> filters = new ArrayList<Object>();

        public CustomFiltersScoreGroup() {

        }

        public CustomFiltersScoreGroup(String scoreMode) {
            this.scoreMode = scoreMode;
        }

        public CustomFiltersScoreGroup add(FilterBuilder filter, String script) {
            filters.add(new CustomFiltersScoreFilter(filter, script));
            return this;
        }

        public CustomFiltersScoreGroup add(FilterBuilder filter, float boost) {
            filters.add(new CustomFiltersScoreFilter(filter, boost));
            return this;
        }

        public CustomFiltersScoreGroup add(CustomFiltersScoreGroup queryBuilder) {
            filters.add(queryBuilder);
            return this;
        }

        public CustomFiltersScoreGroup scoreMode(String scoreMode) {
            this.scoreMode = scoreMode;
            return this;
        }

        protected void doXContent(XContentBuilder builder, Params params, boolean withGrouping) throws IOException {
            if (withGrouping) {
                builder.startObject();
                builder.startArray(scoreMode != null ? scoreMode : "first");
            }
            for (Object item : filters) {
                if (item instanceof CustomFiltersScoreFilter) {
                    ((CustomFiltersScoreFilter) item).doXContent(builder, params);
                } else if (item instanceof CustomFiltersScoreGroup) {
                    ((CustomFiltersScoreGroup) item).doXContent(builder, params, true);
                }
            }
            if (withGrouping) {
                builder.endArray();
                builder.endObject();
            }
        }

    }
}