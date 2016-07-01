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

import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.InternalAggregation.Type;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public abstract class ValuesSourceAggregatorFactory<VS extends ValuesSource, AF extends ValuesSourceAggregatorFactory<VS, AF>>
        extends AggregatorFactory<AF> {

    protected ValuesSourceConfig<VS> config;

    public ValuesSourceAggregatorFactory(String name, Type type, ValuesSourceConfig<VS> config, AggregationContext context,
            AggregatorFactory<?> parent, AggregatorFactories.Builder subFactoriesBuilder, Map<String, Object> metaData) throws IOException {
        super(name, type, context, parent, subFactoriesBuilder, metaData);
        this.config = config;
    }

    public DateTimeZone timeZone() {
        return config.timezone();
        }

    @Override
    public Aggregator createInternal(Aggregator parent, boolean collectsFromSingleBucket,
            List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) throws IOException {
        VS vs = context.valuesSource(config, context.searchContext());
        if (vs == null) {
            return createUnmapped(parent, pipelineAggregators, metaData);
        }
        return doCreateInternal(vs, parent, collectsFromSingleBucket, pipelineAggregators, metaData);
    }

    protected abstract Aggregator createUnmapped(Aggregator parent,
            List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) throws IOException;

    protected abstract Aggregator doCreateInternal(VS valuesSource, Aggregator parent,
            boolean collectsFromSingleBucket, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData)
            throws IOException;

}
