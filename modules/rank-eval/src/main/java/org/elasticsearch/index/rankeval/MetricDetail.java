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

package org.elasticsearch.index.rankeval;

import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Details about a specific {@link EvaluationMetric} that should be included in the resonse.
 */
public interface MetricDetail extends ToXContentObject, NamedWriteable {

    @Override
    default XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startObject(getMetricName());
        innerToXContent(builder, params);
        builder.endObject();
        return builder.endObject();
    }

    default String getMetricName() {
        return getWriteableName();
    }

    /**
     * Implementations should write their own fields to the {@link XContentBuilder} passed in.
     */
    XContentBuilder innerToXContent(XContentBuilder builder, Params params) throws IOException;
}
