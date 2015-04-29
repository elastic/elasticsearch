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

package org.elasticsearch.search.aggregations.reducers.derivative;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.reducers.ReducerBuilder;

import java.io.IOException;

import static org.elasticsearch.search.aggregations.reducers.BucketHelpers.GapPolicy;

public class DerivativeBuilder extends ReducerBuilder<DerivativeBuilder> {

    private String format;
    private GapPolicy gapPolicy;

    public DerivativeBuilder(String name) {
        super(name, DerivativeReducer.TYPE.name());
    }

    public DerivativeBuilder format(String format) {
        this.format = format;
        return this;
    }

    public DerivativeBuilder gapPolicy(GapPolicy gapPolicy) {
        this.gapPolicy = gapPolicy;
        return this;
    }

    @Override
    protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        if (format != null) {
            builder.field(DerivativeParser.FORMAT.getPreferredName(), format);
        }
        if (gapPolicy != null) {
            builder.field(DerivativeParser.GAP_POLICY.getPreferredName(), gapPolicy.getName());
        }
        return builder;
    }

}
