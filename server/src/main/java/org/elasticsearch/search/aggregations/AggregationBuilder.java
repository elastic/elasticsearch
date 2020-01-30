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


import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.QueryShardContext;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

/**
 * A factory that knows how to create an {@link Aggregator} of a specific type.
 */
public abstract class AggregationBuilder
        implements NamedWriteable, ToXContentFragment, BaseAggregationBuilder {

    protected final String name;
    protected AggregatorFactories.Builder factoriesBuilder = AggregatorFactories.builder();

    /**
     * Constructs a new aggregation builder.
     *
     * @param name  The aggregation name
     */
    protected AggregationBuilder(String name) {
        if (name == null) {
            throw new IllegalArgumentException("[name] must not be null: [" + name + "]");
        }
        this.name = name;
    }

    protected AggregationBuilder(AggregationBuilder clone, AggregatorFactories.Builder factoriesBuilder) {
        this.name = clone.name;
        this.factoriesBuilder = factoriesBuilder;
    }

    /** Return this aggregation's name. */
    public String getName() {
        return name;
    }

    /** Internal: build an {@link AggregatorFactory} based on the configuration of this builder. */
    protected abstract AggregatorFactory build(QueryShardContext queryShardContext, AggregatorFactory parent) throws IOException;

    /** Associate metadata with this {@link AggregationBuilder}. */
    @Override
    public abstract AggregationBuilder setMetaData(Map<String, Object> metaData);

    /** Return any associated metadata with this {@link AggregationBuilder}. */
    public abstract Map<String, Object> getMetaData();

    /** Add a sub aggregation to this builder. */
    public abstract AggregationBuilder subAggregation(AggregationBuilder aggregation);

    /** Add a sub aggregation to this builder. */
    public abstract AggregationBuilder subAggregation(PipelineAggregationBuilder aggregation);

    /** Return the configured set of subaggregations **/
    public Collection<AggregationBuilder> getSubAggregations() {
        return factoriesBuilder.getAggregatorFactories();
    }

    /** Return the configured set of pipeline aggregations **/
    public Collection<PipelineAggregationBuilder> getPipelineAggregations() {
        return factoriesBuilder.getPipelineAggregatorFactories();
    }

    /**
     * Internal: Registers sub-factories with this factory. The sub-factory will
     * be responsible for the creation of sub-aggregators under the aggregator
     * created by this factory. This is only for use by
     * {@link AggregatorFactories#parseAggregators(XContentParser)}.
     *
     * @param subFactories
     *            The sub-factories
     * @return this factory (fluent interface)
     */
    @Override
    public abstract AggregationBuilder subAggregations(AggregatorFactories.Builder subFactories);

    /**
     * Create a shallow copy of this builder and replacing {@link #factoriesBuilder} and <code>metaData</code>.
     * Used by {@link #rewrite(QueryRewriteContext)}.
     */
    protected abstract AggregationBuilder shallowCopy(AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metaData);

    public final AggregationBuilder rewrite(QueryRewriteContext context) throws IOException {
        AggregationBuilder rewritten = doRewrite(context);
        AggregatorFactories.Builder rewrittenSubAggs = factoriesBuilder.rewrite(context);
        if (rewritten != this) {
            return rewritten.setMetaData(getMetaData()).subAggregations(rewrittenSubAggs);
        } else if (rewrittenSubAggs != factoriesBuilder) {
            return shallowCopy(rewrittenSubAggs, getMetaData());
        } else {
            return this;
        }
    }

    /**
     * Rewrites this aggregation builder into its primitive form. By default
     * this method return the builder itself. If the builder did not change the
     * identity reference must be returned otherwise the builder will be
     * rewritten infinitely.
     */
    protected AggregationBuilder doRewrite(QueryRewriteContext queryShardContext) throws IOException {
        return this;
    }

    /**
     * Rewrites the given aggregation into its primitive form. Aggregations that for instance fetch resources from remote hosts or
     * can simplify / optimize itself should do their heavy lifting during {@link #rewrite(QueryRewriteContext)}. This method
     * rewrites the aggregation until it doesn't change anymore.
     * @throws IOException if an {@link IOException} occurs
     */
    static AggregationBuilder rewriteAggregation(AggregationBuilder original, QueryRewriteContext context) throws IOException {
        AggregationBuilder builder = original;
        for (AggregationBuilder rewrittenBuilder = builder.rewrite(context); rewrittenBuilder != builder;
             rewrittenBuilder = builder.rewrite(context)) {
            builder = rewrittenBuilder;
        }
        return builder;
    }

    /** Common xcontent fields shared among aggregator builders */
    public static final class CommonFields extends ParseField.CommonFields {
        public static final ParseField VALUE_TYPE = new ParseField("value_type");
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
