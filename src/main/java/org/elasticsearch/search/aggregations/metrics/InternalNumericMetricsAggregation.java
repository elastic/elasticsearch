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

import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.search.aggregations.support.format.ValueFormatter;

import java.util.List;
import java.util.Map;

/**
 *
 */
public abstract class InternalNumericMetricsAggregation extends InternalMetricsAggregation {

    protected ValueFormatter valueFormatter;

    public static abstract class SingleValue extends InternalNumericMetricsAggregation {

        protected SingleValue() {}

        protected SingleValue(String name, Map<String, Object> metaData) {
            super(name, metaData);
        }

        public abstract double value();

        @Override
        public Object getProperty(List<String> path) {
            if (path.isEmpty()) {
                return this;
            } else if (path.size() == 1 && "value".equals(path.get(0))) {
                return value();
            } else {
                throw new ElasticsearchIllegalArgumentException("path not supported for [" + getName() + "]: " + path);
            }
        }

    }

    public static abstract class MultiValue extends InternalNumericMetricsAggregation {

        protected MultiValue() {}

        protected MultiValue(String name, Map<String, Object> metaData) {
            super(name, metaData);
        }

        public abstract double value(String name);

        @Override
        public Object getProperty(List<String> path) {
            if (path.isEmpty()) {
                return this;
            } else if (path.size() == 1) {
                return value(path.get(0));
            } else {
                throw new ElasticsearchIllegalArgumentException("path not supported for [" + getName() + "]: " + path);
            }
        }
    }

    private InternalNumericMetricsAggregation() {} // for serialization

    private InternalNumericMetricsAggregation(String name, Map<String, Object> metaData) {
        super(name, metaData);
    }

}
