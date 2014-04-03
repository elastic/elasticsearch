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
package org.elasticsearch.search.aggregations.support;

import org.elasticsearch.search.aggregations.*;
import org.elasticsearch.search.aggregations.support.format.ValueFormat;

/**
 *
 */
public abstract class ValuesSourceAggregatorFactory<VS extends ValuesSource> extends AggregatorFactory {

    public static abstract class LeafOnly<VS extends ValuesSource> extends ValuesSourceAggregatorFactory<VS> {

        protected LeafOnly(String name, String type, ValuesSourceConfig<VS> valuesSourceConfig) {
            super(name, type, valuesSourceConfig);
        }

        @Override
        public AggregatorFactory subFactories(AggregatorFactories subFactories) {
            throw new AggregationInitializationException("Aggregator [" + name + "] of type [" + type + "] cannot accept sub-aggregations");
        }
    }

    protected ValuesSourceConfig<VS> config;

    protected ValuesSourceAggregatorFactory(String name, String type, ValuesSourceConfig<VS> config) {
        super(name, type);
        this.config = config;
    }

    @Override
    public Aggregator create(AggregationContext context, Aggregator parent, long expectedBucketsCount) {
        if (config.unmapped()) {
            return createUnmapped(context, parent);
        }
        VS vs = context.valuesSource(config, parent == null ? 0 : 1 + parent.depth());
        return create(vs, expectedBucketsCount, context, parent);
    }

    @Override
    public void doValidate() {
        if (config == null || !config.valid()) {
            resolveValuesSourceConfigFromAncestors(name, parent, config.valueSourceType());
        }
    }

    protected abstract Aggregator createUnmapped(AggregationContext aggregationContext, Aggregator parent);

    protected abstract Aggregator create(VS valuesSource, long expectedBucketsCount, AggregationContext aggregationContext, Aggregator parent);

    private void resolveValuesSourceConfigFromAncestors(String aggName, AggregatorFactory parent, Class<VS> requiredValuesSourceType) {
        ValuesSourceConfig config;
        while (parent != null) {
            if (parent instanceof ValuesSourceAggregatorFactory) {
                config = ((ValuesSourceAggregatorFactory) parent).config;
                if (config != null && config.valid()) {
                    if (requiredValuesSourceType == null || requiredValuesSourceType.isAssignableFrom(config.valueSourceType)) {
                        ValueFormat format = config.format;
                        this.config = config;
                        // if the user explicitly defined a format pattern, we'll do our best to keep it even when we inherit the
                        // value source form one of the ancestor aggregations
                        if (this.config.formatPattern != null && format != null && format instanceof ValueFormat.Patternable) {
                            this.config.format = ((ValueFormat.Patternable) format).create(this.config.formatPattern);
                        }
                        return;
                    }
                }
            }
            parent = parent.parent();
        }
        throw new AggregationExecutionException("could not find the appropriate value context to perform aggregation [" + aggName + "]");
    }
}
