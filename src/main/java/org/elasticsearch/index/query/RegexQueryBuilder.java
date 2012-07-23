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
 * Implements the regex search query. Note this query is extremely slow, as it
 * needs to iterate over many terms. Use it only on percolation or on very small indices
 *
 */
public class RegexQueryBuilder extends BaseQueryBuilder implements BoostableQueryBuilder<RegexQueryBuilder> {

    private final String name;

    private final String regex;

    private float boost = -1;

    private String rewrite;

    /**
     * Implements the regex search query. Note this query is extremely slow, as it
     * needs to iterate over many terms. Use it only on percolation or on very small indices
     *
     * @param name     The field name
     * @param regex The regex query string
     */
    public RegexQueryBuilder(String name, String regex) {
        this.name = name;
        this.regex = regex;
    }

    public RegexQueryBuilder rewrite(String rewrite) {
        this.rewrite = rewrite;
        return this;
    }

    /**
     * Sets the boost for this query.  Documents matching this query will (in addition to the normal
     * weightings) have their score multiplied by the boost provided.
     */
    public RegexQueryBuilder boost(float boost) {
        this.boost = boost;
        return this;
    }

    @Override
    public void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(RegexQueryParser.NAME);
        if (boost == -1 && rewrite == null) {
            builder.field(name, regex);
        } else {
            builder.startObject(name);
            builder.field("wildcard", regex);
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