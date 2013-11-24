/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

/**
 *
 */
public abstract class ValueSourceAggregatorFactory<VS extends ValuesSource> extends AggregatorFactory implements ValuesSourceBased {

    public static abstract class LeafOnly<VS extends ValuesSource> extends ValueSourceAggregatorFactory<VS> {

        protected LeafOnly(String name, String type, ValuesSourceConfig<VS> valuesSourceConfig) {
            super(name, type, valuesSourceConfig);
        }

        @Override
        public AggregatorFactory subFactories(AggregatorFactories subFactories) {
            throw new AggregationInitializationException("Aggregator [" + name + "] of type [" + type + "] cannot accept sub-aggregations");
        }
    }

    protected ValuesSourceConfig<VS> valuesSourceConfig;

    protected ValueSourceAggregatorFactory(String name, String type, ValuesSourceConfig<VS> valuesSourceConfig) {
        super(name, type);
        this.valuesSourceConfig = valuesSourceConfig;
    }

    @Override
    public ValuesSourceConfig valuesSourceConfig() {
        return valuesSourceConfig;
    }

    @Override
    public Aggregator create(AggregationContext context, Aggregator parent, long expectedBucketsCount) {
        if (valuesSourceConfig.unmapped()) {
            return createUnmapped(context, parent);
        }
        VS vs = context.valuesSource(valuesSourceConfig, parent == null ? 0 : 1 + parent.depth());
        return create(vs, expectedBucketsCount, context, parent);
    }

    @Override
    public void doValidate() {
        if (valuesSourceConfig == null || !valuesSourceConfig.valid()) {
            valuesSourceConfig = resolveValuesSourceConfigFromAncestors(name, parent, valuesSourceConfig.valueSourceType());
        }
    }

    protected abstract Aggregator createUnmapped(AggregationContext aggregationContext, Aggregator parent);

    protected abstract Aggregator create(VS valuesSource, long expectedBucketsCount, AggregationContext aggregationContext, Aggregator parent);

    private static <VS extends ValuesSource> ValuesSourceConfig<VS> resolveValuesSourceConfigFromAncestors(String aggName, AggregatorFactory parent, Class<VS> requiredValuesSourceType) {
        ValuesSourceConfig config;
        while (parent != null) {
            if (parent instanceof ValuesSourceBased) {
                config = ((ValuesSourceBased) parent).valuesSourceConfig();
                if (config != null && config.valid()) {
                    if (requiredValuesSourceType == null || requiredValuesSourceType.isAssignableFrom(config.valueSourceType())) {
                        return (ValuesSourceConfig<VS>) config;
                    }
                }
            }
            parent = parent.parent();
        }
        throw new AggregationExecutionException("could not find the appropriate value context to perform aggregation [" + aggName + "]");
    }
}
