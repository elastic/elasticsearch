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

package org.elasticsearch.index.query.functionscore;

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

public abstract class ScoreFunctionBuilder implements ToXContent {

    public ScoreFunctionBuilder setWeight(float weight) {
        this.weight = weight;
        return this;
    }

    private Float weight;

    public abstract String getName();

    protected void buildWeight(XContentBuilder builder) throws IOException {
        if (weight != null) {
            builder.field(FunctionScoreQueryParser.WEIGHT_FIELD.getPreferredName(), weight);
        }
    }

    @Override
    public final XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        buildWeight(builder);
        doXContent(builder, params);
        return builder;
    }

    protected abstract void doXContent(XContentBuilder builder, Params params) throws IOException;
}
