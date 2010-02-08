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

package org.elasticsearch.index.query.json;

import org.elasticsearch.index.query.QueryBuilderException;
import org.elasticsearch.util.json.JsonBuilder;

import java.io.IOException;

/**
 * @author kimchy (Shay Banon)
 */
public class SpanNotJsonQueryBuilder extends BaseJsonQueryBuilder implements JsonSpanQueryBuilder {

    private JsonSpanQueryBuilder include;

    private JsonSpanQueryBuilder exclude;

    private float boost = -1;

    public SpanNotJsonQueryBuilder include(JsonSpanQueryBuilder include) {
        this.include = include;
        return this;
    }

    public SpanNotJsonQueryBuilder exclude(JsonSpanQueryBuilder exclude) {
        this.exclude = exclude;
        return this;
    }

    public SpanNotJsonQueryBuilder boost(float boost) {
        this.boost = boost;
        return this;
    }

    @Override protected void doJson(JsonBuilder builder) throws IOException {
        if (include == null) {
            throw new QueryBuilderException("Must specify include when using spanNot query");
        }
        if (exclude == null) {
            throw new QueryBuilderException("Must specify exclude when using spanNot query");
        }
        builder.startObject(SpanNotJsonQueryParser.NAME);
        builder.field("include");
        include.toJson(builder);
        builder.field("exclude");
        exclude.toJson(builder);
        if (boost == -1) {
            builder.field("boost", boost);
        }
        builder.endObject();
    }
}
