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

import org.apache.lucene.search.Query;
import org.apache.lucene.search.spans.SpanNotQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

/**
 *
 */
public class SpanNotQueryBuilder extends BaseQueryBuilder implements SpanQueryBuilder, BoostableQueryBuilder<SpanNotQueryBuilder> {

    private SpanQueryBuilder include;

    private SpanQueryBuilder exclude;

    private Integer dist;

    private Integer pre;

    private Integer post;

    private Float boost;

    private String queryName;

    public static final String NAME = "span_not";

    @Inject
    public SpanNotQueryBuilder() {
    }

    public SpanNotQueryBuilder include(SpanQueryBuilder include) {
        this.include = include;
        return this;
    }

    public SpanNotQueryBuilder exclude(SpanQueryBuilder exclude) {
        this.exclude = exclude;
        return this;
    }

    public SpanNotQueryBuilder dist(int dist) {
        this.dist = dist;
        return this;
    }

    public SpanNotQueryBuilder pre(int pre) {
        this.pre = (pre >=0) ? pre : 0;
        return this;
    }

    public SpanNotQueryBuilder post(int post) {
        this.post = (post >= 0) ? post : 0;
        return this;
    }

    @Override
    public SpanNotQueryBuilder boost(float boost) {
        this.boost = boost;
        return this;
    }

    /**
     * Sets the query name for the filter that can be used when searching for matched_filters per hit.
     * @param queryName The query name
     * @return this
     */
    public SpanNotQueryBuilder queryName(String queryName) {
        this.queryName = queryName;
        return this;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        if (include == null) {
            throw new ElasticsearchIllegalArgumentException("Must specify include when using spanNot query");
        }
        if (exclude == null) {
            throw new ElasticsearchIllegalArgumentException("Must specify exclude when using spanNot query");
        }

        if (dist != null && (pre != null || post != null)) {
             throw new ElasticsearchIllegalArgumentException("spanNot can either use [dist] or [pre] & [post] (or none)");
        }

        builder.startObject(SpanNotQueryBuilder.NAME);
        builder.field("include");
        include.toXContent(builder, params);
        builder.field("exclude");
        exclude.toXContent(builder, params);
        if (dist != null) {
            builder.field("dist", dist);
        }
        if (pre != null) {
            builder.field("pre", pre);
        }
        if (post != null) {
            builder.field("post", post);
        }
        if (boost != null) {
            builder.field("boost", boost);
        }
        if (queryName != null) {
            builder.field("_name", queryName);
        }
        builder.endObject();
    }

    @Override
    public String[] names() {
        return new String[]{NAME, Strings.toCamelCase(NAME)};
    }

    @Override
    public Query parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();

        float boost = 1.0f;

        SpanQuery include = null;
        SpanQuery exclude = null;

        Integer dist = null;
        Integer pre  = null;
        Integer post = null;

        String queryName = null;

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("include".equals(currentFieldName)) {
                    Query query = parseContext.parseInnerQuery();
                    if (!(query instanceof SpanQuery)) {
                        throw new QueryParsingException(parseContext.index(), "spanNot [include] must be of type span query");
                    }
                    include = (SpanQuery) query;
                } else if ("exclude".equals(currentFieldName)) {
                    Query query = parseContext.parseInnerQuery();
                    if (!(query instanceof SpanQuery)) {
                        throw new QueryParsingException(parseContext.index(), "spanNot [exclude] must be of type span query");
                    }
                    exclude = (SpanQuery) query;
                } else {
                    throw new QueryParsingException(parseContext.index(), "[span_not] query does not support [" + currentFieldName + "]");
                }
            } else {
                if ("dist".equals(currentFieldName)) {
                    dist = parser.intValue();
                } else if ("pre".equals(currentFieldName)) {
                    pre = parser.intValue();
                } else if ("post".equals(currentFieldName)) {
                    post = parser.intValue();
                } else if ("boost".equals(currentFieldName)) {
                    boost = parser.floatValue();
                } else if ("_name".equals(currentFieldName)) {
                    queryName = parser.text();
                } else {
                    throw new QueryParsingException(parseContext.index(), "[span_not] query does not support [" + currentFieldName + "]");
                }
            }
        }
        if (include == null) {
            throw new QueryParsingException(parseContext.index(), "spanNot must have [include] span query clause");
        }
        if (exclude == null) {
            throw new QueryParsingException(parseContext.index(), "spanNot must have [exclude] span query clause");
        }
        if (dist != null && (pre != null || post != null)) {
            throw new QueryParsingException(parseContext.index(), "spanNot can either use [dist] or [pre] & [post] (or none)");
        }

        // set appropriate defaults
        if (pre != null && post == null) {
            post = 0;
        } else if (pre == null && post != null){
            pre = 0;
        }

        SpanNotQuery query;
        if (pre != null && post != null) {
            query = new SpanNotQuery(include, exclude, pre, post);
        } else if (dist != null) {
            query = new SpanNotQuery(include, exclude, dist);
        } else {
            query = new SpanNotQuery(include, exclude);
        }

        query.setBoost(boost);
        if (queryName != null) {
            parseContext.addNamedQuery(queryName, query);
        }
        return query;
    }
}
