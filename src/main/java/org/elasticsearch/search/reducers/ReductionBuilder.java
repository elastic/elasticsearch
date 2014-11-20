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

package org.elasticsearch.search.reducers;

import com.google.common.collect.Lists;
import org.elasticsearch.ElasticsearchGenerationException;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * A base class for all bucket reduction builders.
 */
public abstract class ReductionBuilder<B extends ReductionBuilder<B>> extends AbstractReductionBuilder {

    private List<AbstractReductionBuilder> reductions;
    private BytesReference reductionsBinary;

    /**
     * Sole constructor, typically used by sub-classes.
     */
    protected ReductionBuilder(String name, String type) {
        super(name, type);
    }

    /**
     * Add a sub get to this bucket get.
     */
    @SuppressWarnings("unchecked")
    public B subAggregation(AbstractReductionBuilder aggregation) {
        if (reductions == null) {
            reductions = Lists.newArrayList();
        }
        reductions.add(aggregation);
        return (B) this;
    }

    /**
     * Sets a raw (xcontent / json) sub addReduction.
     */
    public B subReduction(byte[] reductionsBinary) {
        return subReduction(reductionsBinary, 0, reductionsBinary.length);
    }

    /**
     * Sets a raw (xcontent / json) sub addReduction.
     */
    public B subReduction(byte[] reductionsBinary, int reductionsBinaryOffset, int reductionsBinaryLength) {
        return subReduction(new BytesArray(reductionsBinary, reductionsBinaryOffset, reductionsBinaryLength));
    }

    /**
     * Sets a raw (xcontent / json) sub addReduction.
     */
    @SuppressWarnings("unchecked")
    public B subReduction(BytesReference reductionsBinary) {
        this.reductionsBinary = reductionsBinary;
        return (B) this;
    }

    /**
     * Sets a raw (xcontent / json) sub addReduction.
     */
    public B subReduction(XContentBuilder aggs) {
        return subReduction(aggs.bytes());
    }

    /**
     * Sets a raw (xcontent / json) sub addReduction.
     */
    public B subReduction(Map<String, Object> reductions) {
        try {
            XContentBuilder builder = XContentFactory.contentBuilder(Requests.CONTENT_TYPE);
            builder.map(reductions);
            return subReduction(builder);
        } catch (IOException e) {
            throw new ElasticsearchGenerationException("Failed to generate [" + reductions + "]", e);
        }
    }

    @Override
    public final XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(getName());

        builder.field(type);
        internalXContent(builder, params);

        if (reductions != null || reductionsBinary != null) {
            builder.startArray("reductions");

            if (reductions != null) {
                for (AbstractReductionBuilder subReduction : reductions) {
                    builder.startObject();
                    subReduction.toXContent(builder, params);
                    builder.endObject();
                }
            }

            if (reductionsBinary != null) {
                if (XContentFactory.xContentType(reductionsBinary) == builder.contentType()) {
                    builder.rawField("reductions", reductionsBinary);
                } else {
                    builder.field("reductions_binary", reductionsBinary);
                }
            }

            builder.endObject();
        }

        return builder.endObject();
    }

    protected abstract XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException;
}
