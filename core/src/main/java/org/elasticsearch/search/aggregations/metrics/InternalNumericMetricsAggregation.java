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

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public abstract class InternalNumericMetricsAggregation extends InternalAggregation {

    private static final DocValueFormat DEFAULT_FORMAT = DocValueFormat.RAW;

    protected DocValueFormat format = DEFAULT_FORMAT;

    public abstract static class SingleValue extends InternalNumericMetricsAggregation implements NumericMetricsAggregation.SingleValue {
        protected SingleValue(String name, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) {
            super(name, pipelineAggregators, metaData);
        }

        /**
         * Read from a stream.
         */
        protected SingleValue(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public String getValueAsString() {
            return format.format(value());
        }

        @Override
        public Object getProperty(List<String> path) {
            if (path.isEmpty()) {
                return this;
            } else if (path.size() == 1 && "value".equals(path.get(0))) {
                return value();
            } else {
                throw new IllegalArgumentException("path not supported for [" + getName() + "]: " + path);
            }
        }

    }

    public abstract static class MultiValue extends InternalNumericMetricsAggregation implements NumericMetricsAggregation.MultiValue {
        protected MultiValue(String name, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) {
            super(name, pipelineAggregators, metaData);
        }

        /**
         * Read from a stream.
         */
        protected MultiValue(StreamInput in) throws IOException {
            super(in);
        }

        public abstract double value(String name);

        public String valueAsString(String name) {
            return format.format(value(name));
        }

        @Override
        public Object getProperty(List<String> path) {
            if (path.isEmpty()) {
                return this;
            } else if (path.size() == 1) {
                return value(path.get(0));
            } else {
                throw new IllegalArgumentException("path not supported for [" + getName() + "]: " + path);
            }
        }
    }

    private InternalNumericMetricsAggregation(String name, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) {
        super(name, pipelineAggregators, metaData);
    }

    /**
     * Read from a stream.
     */
    protected InternalNumericMetricsAggregation(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public int hashCode() {
        return Objects.hash(format, super.hashCode());
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj.getClass() != getClass()) {
            return false;
        }
        InternalNumericMetricsAggregation other = (InternalNumericMetricsAggregation) obj;
        return super.equals(obj) &&
                Objects.equals(format, other.format);
    }
}
