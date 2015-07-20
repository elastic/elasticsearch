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

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.script.Script;

import java.io.IOException;

/**
 *
 */
public abstract class ValuesSourceMetricsAggregationBuilder<B extends ValuesSourceMetricsAggregationBuilder<B>> extends MetricsAggregationBuilder<B> {

    private String field;
    private Script script;
    private String format;
    private Object missing;

    protected ValuesSourceMetricsAggregationBuilder(String name, String type) {
        super(name, type);
    }

    @SuppressWarnings("unchecked")
    public B field(String field) {
        this.field = field;
        return (B) this;
    }

    /**
     * The script to use for this aggregation
     */
    @SuppressWarnings("unchecked")
    public B script(Script script) {
        this.script = script;
        return (B) this;
    }

    @SuppressWarnings("unchecked")
    public B format(String format) {
        this.format = format;
        return (B) this;
    }

    /**
     * Configure the value to use when documents miss a value.
     */
    public B missing(Object missingValue) {
        this.missing = missingValue;
        return (B) this;
    }

    @Override
    protected void internalXContent(XContentBuilder builder, Params params) throws IOException {
        if (field != null) {
            builder.field("field", field);
        }

        if (script != null) {
            builder.field("script", script);
        }

        if (format != null) {
            builder.field("format", format);
        }

        if (missing != null) {
            builder.field("missing", missing);
        }
    }
}
