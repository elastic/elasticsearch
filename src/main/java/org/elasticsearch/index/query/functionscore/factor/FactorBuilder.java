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

package org.elasticsearch.index.query.functionscore.factor;

import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilder;

import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * A query that simply applies the boost factor to another query (multiply it).
 * 
 * 
 */
public class FactorBuilder implements ScoreFunctionBuilder {

    private Float boostFactor;

    /**
     * Sets the boost factor for this query.
     */
    public FactorBuilder boostFactor(float boost) {
        this.boostFactor = new Float(boost);
        return this;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (boostFactor != null) {
            builder.field("boost_factor", boostFactor.floatValue());
        }
        return builder;
    }

    @Override
    public String getName() {
        return FactorParser.NAMES[0];
    }
}