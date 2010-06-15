/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

package org.elasticsearch.index.query.xcontent;

import org.elasticsearch.common.xcontent.builder.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilderException;

import java.io.IOException;

/**
 * @author kimchy (shay.banon)
 */
public class SpanNotQueryBuilder extends BaseQueryBuilder implements XContentSpanQueryBuilder {

    private XContentSpanQueryBuilder include;

    private XContentSpanQueryBuilder exclude;

    private float boost = -1;

    public SpanNotQueryBuilder include(XContentSpanQueryBuilder include) {
        this.include = include;
        return this;
    }

    public SpanNotQueryBuilder exclude(XContentSpanQueryBuilder exclude) {
        this.exclude = exclude;
        return this;
    }

    public SpanNotQueryBuilder boost(float boost) {
        this.boost = boost;
        return this;
    }

    @Override protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        if (include == null) {
            throw new QueryBuilderException("Must specify include when using spanNot query");
        }
        if (exclude == null) {
            throw new QueryBuilderException("Must specify exclude when using spanNot query");
        }
        builder.startObject(SpanNotQueryParser.NAME);
        builder.field("include");
        include.toXContent(builder, params);
        builder.field("exclude");
        exclude.toXContent(builder, params);
        if (boost == -1) {
            builder.field("boost", boost);
        }
        builder.endObject();
    }
}
