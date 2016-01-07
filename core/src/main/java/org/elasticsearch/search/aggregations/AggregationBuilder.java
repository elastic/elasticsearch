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

package org.elasticsearch.search.aggregations;

import org.elasticsearch.ElasticsearchGenerationException;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregatorFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A base class for all bucket aggregation builders. NORELEASE REMOVE WHEN AGG
 * REFACTORING IS COMPLETE
 */
@Deprecated
public abstract class AggregationBuilder<B extends AggregationBuilder<B>> extends AbstractAggregationBuilder {

    private List<AbstractAggregationBuilder> aggregations;
    private List<AggregatorFactory> aggregatorFactories;
    private List<PipelineAggregatorFactory> pipelineAggregatorFactories;
    private BytesReference aggregationsBinary;
    private Map<String, Object> metaData;

    /**
     * Sole constructor, typically used by sub-classes.
     */
    protected AggregationBuilder(String name, String type) {
        super(name, type);
    }

    /**
     * Add a sub aggregation to this aggregation. NORELEASE REMOVE THIS WHEN AGG
     * REFACTOR IS COMPLETE
     */
    @SuppressWarnings("unchecked")
    public B subAggregation(AbstractAggregationBuilder aggregation) {
        if (aggregations == null) {
            aggregations = new ArrayList<>();
        }
        aggregations.add(aggregation);
        return (B) this;
    }

    /**
     * Add a sub aggregation to this aggregation.
     */
    @SuppressWarnings("unchecked")
    public B subAggregation(AggregatorFactory aggregation) {
        if (aggregatorFactories == null) {
            aggregatorFactories = new ArrayList<>();
        }
        aggregatorFactories.add(aggregation);
        return (B) this;
    }

    /**
     * Add a sub aggregation to this aggregation.
     */
    @SuppressWarnings("unchecked")
    public B subAggregation(PipelineAggregatorFactory aggregation) {
        if (pipelineAggregatorFactories == null) {
            pipelineAggregatorFactories = new ArrayList<>();
        }
        pipelineAggregatorFactories.add(aggregation);
        return (B) this;
    }

    /**
     * Sets a raw (xcontent / json) sub addAggregation.
     */
    @Deprecated
    public B subAggregation(byte[] aggregationsBinary) {
        return subAggregation(aggregationsBinary, 0, aggregationsBinary.length);
    }

    /**
     * Sets a raw (xcontent / json) sub addAggregation.
     */
    @Deprecated
    public B subAggregation(byte[] aggregationsBinary, int aggregationsBinaryOffset, int aggregationsBinaryLength) {
        return subAggregation(new BytesArray(aggregationsBinary, aggregationsBinaryOffset, aggregationsBinaryLength));
    }

    /**
     * Sets a raw (xcontent / json) sub addAggregation.
     */
    @Deprecated
    @SuppressWarnings("unchecked")
    public B subAggregation(BytesReference aggregationsBinary) {
        this.aggregationsBinary = aggregationsBinary;
        return (B) this;
    }

    /**
     * Sets a raw (xcontent / json) sub addAggregation.
     */
    @Deprecated
    public B subAggregation(XContentBuilder aggs) {
        return subAggregation(aggs.bytes());
    }

    /**
     * Sets a raw (xcontent / json) sub addAggregation.
     */
    @Deprecated
    public B subAggregation(Map<String, Object> aggs) {
        try {
            XContentBuilder builder = XContentFactory.contentBuilder(Requests.CONTENT_TYPE);
            builder.map(aggs);
            return subAggregation(builder);
        } catch (IOException e) {
            throw new ElasticsearchGenerationException("Failed to generate [" + aggs + "]", e);
        }
    }

    /**
     * Sets the meta data to be included in the aggregation response
     */
    public B setMetaData(Map<String, Object> metaData) {
        this.metaData = metaData;
        return (B)this;
    }

    @Override
    public final XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(getName());

        if (this.metaData != null) {
            builder.field("meta", this.metaData);
        }
        builder.field(type);
        internalXContent(builder, params);

        if (aggregations != null || aggregatorFactories != null || pipelineAggregatorFactories != null || aggregationsBinary != null) {

            if (aggregations != null || aggregatorFactories != null || pipelineAggregatorFactories != null) {
                builder.startObject("aggregations");
                if (aggregations != null) {
                    for (AbstractAggregationBuilder subAgg : aggregations) {
                        subAgg.toXContent(builder, params);
                    }
                }
                if (aggregatorFactories != null) {
                    for (AggregatorFactory subAgg : aggregatorFactories) {
                        subAgg.toXContent(builder, params);
                    }
                }
                if (pipelineAggregatorFactories != null) {
                    for (PipelineAggregatorFactory subAgg : pipelineAggregatorFactories) {
                        subAgg.toXContent(builder, params);
                    }
                }
                builder.endObject();
            }

            if (aggregationsBinary != null) {
                if (XContentFactory.xContentType(aggregationsBinary) == builder.contentType()) {
                    builder.rawField("aggregations", aggregationsBinary);
                } else {
                    builder.field("aggregations_binary", aggregationsBinary);
                }
            }

        }

        return builder.endObject();
    }

    protected abstract XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException;
}
