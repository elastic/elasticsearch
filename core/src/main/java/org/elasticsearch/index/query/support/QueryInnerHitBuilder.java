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

package org.elasticsearch.index.query.support;

import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

/**
 */
public class QueryInnerHitBuilder extends BaseInnerHitBuilder<QueryInnerHitBuilder> {

    private String name;

    /**
     * Set the key name to be used in the response.
     *
     * Defaults to the path if used in nested query, child type if used in has_child query and parent type if used in has_parent.
     */
    public QueryInnerHitBuilder setName(String name) {
        this.name = name;
        return this;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        super.toXContent(builder, params);
        if (name != null) {
            builder.field("name", name);
        }
        return builder;
    }

}
