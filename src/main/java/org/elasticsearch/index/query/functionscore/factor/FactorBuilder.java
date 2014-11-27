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

package org.elasticsearch.index.query.functionscore.factor;

import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.lucene.search.function.BoostScoreFunction;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilder;

import java.io.IOException;

/**
 * A query that simply applies the boost factor to another query (multiply it).
 *
 *
 */
@Deprecated
public class FactorBuilder extends ScoreFunctionBuilder {

    private Float boostFactor;

    /**
     * Sets the boost factor for this query.
     */
    public FactorBuilder boostFactor(float boost) {
        this.boostFactor = new Float(boost);
        return this;
    }

    @Override
    public void doXContent(XContentBuilder builder, Params params) throws IOException {
        if (boostFactor != null) {
            builder.field("boost_factor", boostFactor.floatValue());
        }
    }

    @Override
    public String getName() {
        return FactorParser.NAMES[0];
    }

    @Override
    public ScoreFunctionBuilder setWeight(float weight) {
        throw new ElasticsearchIllegalArgumentException(BoostScoreFunction.BOOST_WEIGHT_ERROR_MESSAGE);
    }

    @Override
    public void buildWeight(XContentBuilder builder) throws IOException {
        //we do not want the weight to be written for boost_factor as it does not make sense to have it
    }
}