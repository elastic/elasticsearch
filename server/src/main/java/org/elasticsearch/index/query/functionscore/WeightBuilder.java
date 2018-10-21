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

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.search.function.ScoreFunction;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryShardContext;

import java.io.IOException;

/**
 * A query that multiplies the weight to the score.
 */
public class WeightBuilder extends ScoreFunctionBuilder<WeightBuilder> {
    public static final String NAME = "weight";

    /**
     * Standard constructor.
     */
    public WeightBuilder() {
    }

    /**
     * Read from a stream.
     */
    public WeightBuilder(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
    }

    @Override
    protected boolean doEquals(WeightBuilder functionBuilder) {
        return true;
    }

    @Override
    protected int doHashCode() {
        return 0;
    }

    @Override
    protected ScoreFunction doToFunction(QueryShardContext context) throws IOException {
        //nothing to do here, weight will be applied by the parent class, no score function
        return null;
    }
}
