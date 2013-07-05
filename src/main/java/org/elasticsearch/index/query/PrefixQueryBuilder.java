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

import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * A Query that matches documents containing terms with a specified prefix.
 *
 *
 */
public class PrefixQueryBuilder extends BaseQueryBuilder implements MultiTermQueryBuilder, BoostableQueryBuilder<PrefixQueryBuilder> {

    private final String name;

    private final String prefix;

    private float boost = -1;

    private String rewrite;

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

    /**
     * Sets the boost for this query.  Documents matching this query will (in addition to the normal
     * weightings) have their score multiplied by the boost provided.
     */
    public PrefixQueryBuilder boost(float boost) {
        this.boost = boost;
        return this;
    }

    public PrefixQueryBuilder rewrite(String rewrite) {
        this.rewrite = rewrite;
        return this;
    }

    @Override
    public void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(PrefixQueryParser.NAME);
        if (boost == -1 && rewrite == null) {
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
            builder.endObject();
        }
        builder.endObject();
    }
}