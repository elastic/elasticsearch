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

package org.elasticsearch.search.reducers.metric.sum;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.reducers.ReductionBuilder;

import java.io.IOException;

public class SumBuilder extends ReductionBuilder<SumBuilder> {

    private String buckets = null;
    private String field = null;

    public SumBuilder(String name) {
        super(name, InternalSum.TYPE.name());
    }

    public SumBuilder buckets(String buckets) {
        this.buckets = buckets;
        return this;
    }

    public SumBuilder field(String field) {
        this.field = field;
        return this;
    }

    @Override
    protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        if (buckets != null) {
            builder.field(SumParser.BUCKETS_FIELD.getPreferredName(), buckets);
        }

        if (field != null) {
            builder.field(SumParser.FIELD_NAME_FIELD.getPreferredName(), field);
        }

        builder.endObject();
        return builder;
    }

}
