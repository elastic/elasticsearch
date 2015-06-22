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

package org.elasticsearch.search.aggregations.pipeline.cumulativesum;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregatorBuilder;

import java.io.IOException;

public class CumulativeSumBuilder extends PipelineAggregatorBuilder<CumulativeSumBuilder> {

    private String format;

    public CumulativeSumBuilder(String name) {
        super(name, CumulativeSumPipelineAggregator.TYPE.name());
    }

    public CumulativeSumBuilder format(String format) {
        this.format = format;
        return this;
    }

    @Override
    protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        if (format != null) {
            builder.field(CumulativeSumParser.FORMAT.getPreferredName(), format);
        }
        return builder;
    }

}
