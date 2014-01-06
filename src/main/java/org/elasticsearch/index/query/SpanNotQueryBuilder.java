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

import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

/**
 *
 */
public class SpanNotQueryBuilder extends BaseQueryBuilder implements SpanQueryBuilder, BoostableQueryBuilder<SpanNotQueryBuilder> {

    private SpanQueryBuilder include;

    private SpanQueryBuilder exclude;

    private float boost = -1;

    private String queryName;

    public SpanNotQueryBuilder include(SpanQueryBuilder include) {
        this.include = include;
        return this;
    }

    public SpanNotQueryBuilder exclude(SpanQueryBuilder exclude) {
        this.exclude = exclude;
        return this;
    }

    public SpanNotQueryBuilder boost(float boost) {
        this.boost = boost;
        return this;
    }

    /**
     * Sets the query name for the filter that can be used when searching for matched_filters per hit.
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
        builder.startObject(SpanNotQueryParser.NAME);
        builder.field("include");
        include.toXContent(builder, params);
        builder.field("exclude");
        exclude.toXContent(builder, params);
        if (boost != -1) {
            builder.field("boost", boost);
        }
        if (queryName != null) {
            builder.field("_name", queryName);
        }
        builder.endObject();
    }
}
