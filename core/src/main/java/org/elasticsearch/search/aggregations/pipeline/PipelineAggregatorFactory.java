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
package org.elasticsearch.search.aggregations.pipeline;

import org.elasticsearch.search.aggregations.AggregatorFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * A factory that knows how to create an {@link PipelineAggregator} of a
 * specific type.
 */
public abstract class PipelineAggregatorFactory {

    protected String name;
    protected String type;
    protected String[] bucketsPaths;
    protected Map<String, Object> metaData;

    /**
     * Constructs a new pipeline aggregator factory.
     *
     * @param name
     *            The aggregation name
     * @param type
     *            The aggregation type
     */
    public PipelineAggregatorFactory(String name, String type, String[] bucketsPaths) {
        this.name = name;
        this.type = type;
        this.bucketsPaths = bucketsPaths;
    }

    public String name() {
        return name;
    }

    /**
     * Validates the state of this factory (makes sure the factory is properly
     * configured)
     *
     * @param pipelineAggregatorFactories
     * @param factories
     * @param parent
     */
    public final void validate(AggregatorFactory parent, AggregatorFactory[] factories,
            List<PipelineAggregatorFactory> pipelineAggregatorFactories) {
        doValidate(parent, factories, pipelineAggregatorFactories);
    }

    protected abstract PipelineAggregator createInternal(Map<String, Object> metaData) throws IOException;

    /**
     * Creates the pipeline aggregator
     *
     * @param context
     *            The aggregation context
     * @param parent
     *            The parent aggregator (if this is a top level factory, the
     *            parent will be {@code null})
     * @param collectsFromSingleBucket
     *            If true then the created aggregator will only be collected
     *            with <tt>0</tt> as a bucket ordinal. Some factories can take
     *            advantage of this in order to return more optimized
     *            implementations.
     *
     * @return The created aggregator
     */
    public final PipelineAggregator create() throws IOException {
        PipelineAggregator aggregator = createInternal(this.metaData);
        return aggregator;
    }

    public void doValidate(AggregatorFactory parent, AggregatorFactory[] factories,
            List<PipelineAggregatorFactory> pipelineAggregatorFactories) {
    }

    public void setMetaData(Map<String, Object> metaData) {
        this.metaData = metaData;
    }

    public String getName() {
        return name;
    }

    public String[] getBucketsPaths() {
        return bucketsPaths;
    }

}
