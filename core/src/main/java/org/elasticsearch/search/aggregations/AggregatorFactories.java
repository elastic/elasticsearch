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

import org.elasticsearch.action.support.ToXContentToBytes;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.AggregationPath;
import org.elasticsearch.search.aggregations.support.AggregationPath.PathElement;
import org.elasticsearch.search.profile.Profilers;
import org.elasticsearch.search.profile.aggregation.ProfilingAggregator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 *
 */
public class AggregatorFactories {

    public static final AggregatorFactories EMPTY = new AggregatorFactories(null, new AggregatorFactory<?>[0],
            new ArrayList<PipelineAggregationBuilder>());

    private AggregatorFactory<?> parent;
    private AggregatorFactory<?>[] factories;
    private List<PipelineAggregationBuilder> pipelineAggregatorFactories;

    public static Builder builder() {
        return new Builder();
    }

    private AggregatorFactories(AggregatorFactory<?> parent, AggregatorFactory<?>[] factories,
            List<PipelineAggregationBuilder> pipelineAggregators) {
        this.parent = parent;
        this.factories = factories;
        this.pipelineAggregatorFactories = pipelineAggregators;
    }

    public List<PipelineAggregator> createPipelineAggregators() throws IOException {
        List<PipelineAggregator> pipelineAggregators = new ArrayList<>();
        for (PipelineAggregationBuilder factory : this.pipelineAggregatorFactories) {
            pipelineAggregators.add(factory.create());
        }
        return pipelineAggregators;
    }

    /**
     * Create all aggregators so that they can be consumed with multiple
     * buckets.
     */
    public Aggregator[] createSubAggregators(Aggregator parent) throws IOException {
        Aggregator[] aggregators = new Aggregator[countAggregators()];
        for (int i = 0; i < factories.length; ++i) {
            // TODO: sometimes even sub aggregations always get called with bucket 0, eg. if
            // you have a terms agg under a top-level filter agg. We should have a way to
            // propagate the fact that only bucket 0 will be collected with single-bucket
            // aggs
            final boolean collectsFromSingleBucket = false;
            Aggregator factory = factories[i].create(parent, collectsFromSingleBucket);
            Profilers profilers = factory.context().searchContext().getProfilers();
            if (profilers != null) {
                factory = new ProfilingAggregator(factory, profilers.getAggregationProfiler());
            }
            aggregators[i] = factory;
        }
        return aggregators;
    }

    public Aggregator[] createTopLevelAggregators() throws IOException {
        // These aggregators are going to be used with a single bucket ordinal, no need to wrap the PER_BUCKET ones
        Aggregator[] aggregators = new Aggregator[factories.length];
        for (int i = 0; i < factories.length; i++) {
            // top-level aggs only get called with bucket 0
            final boolean collectsFromSingleBucket = true;
            Aggregator factory = factories[i].create(null, collectsFromSingleBucket);
            Profilers profilers = factory.context().searchContext().getProfilers();
            if (profilers != null) {
                factory = new ProfilingAggregator(factory, profilers.getAggregationProfiler());
            }
            aggregators[i] = factory;
        }
        return aggregators;
    }

    /**
     * @return the number of sub-aggregator factories not including pipeline
     *         aggregator factories
     */
    public int countAggregators() {
        return factories.length;
    }

    /**
     * @return the number of pipeline aggregator factories
     */
    public int countPipelineAggregators() {
        return pipelineAggregatorFactories.size();
    }

    public void validate() {
        for (AggregatorFactory<?> factory : factories) {
            factory.validate();
        }
        for (PipelineAggregationBuilder factory : pipelineAggregatorFactories) {
            factory.validate(parent, factories, pipelineAggregatorFactories);
        }
    }

    public static class Builder extends ToXContentToBytes implements Writeable {
        private final Set<String> names = new HashSet<>();
        private final List<AggregationBuilder> aggregationBuilders = new ArrayList<>();
        private final List<PipelineAggregationBuilder> pipelineAggregatorBuilders = new ArrayList<>();
        private boolean skipResolveOrder;

        /**
         * Create an empty builder.
         */
        public Builder() {
        }

        /**
         * Read from a stream.
         */
        public Builder(StreamInput in) throws IOException {
            int factoriesSize = in.readVInt();
            for (int i = 0; i < factoriesSize; i++) {
                addAggregator(in.readNamedWriteable(AggregationBuilder.class));
            }
            int pipelineFactoriesSize = in.readVInt();
            for (int i = 0; i < pipelineFactoriesSize; i++) {
                addPipelineAggregator(in.readNamedWriteable(PipelineAggregationBuilder.class));
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(this.aggregationBuilders.size());
            for (AggregationBuilder factory : aggregationBuilders) {
                out.writeNamedWriteable(factory);
            }
            out.writeVInt(this.pipelineAggregatorBuilders.size());
            for (PipelineAggregationBuilder factory : pipelineAggregatorBuilders) {
                out.writeNamedWriteable(factory);
            }
        }

        public Builder addAggregators(AggregatorFactories factories) {
            throw new UnsupportedOperationException("This needs to be removed");
        }

        public Builder addAggregator(AggregationBuilder factory) {
            if (!names.add(factory.name)) {
                throw new IllegalArgumentException("Two sibling aggregations cannot have the same name: [" + factory.name + "]");
            }
            aggregationBuilders.add(factory);
            return this;
        }

        public Builder addPipelineAggregator(PipelineAggregationBuilder pipelineAggregatorFactory) {
            this.pipelineAggregatorBuilders.add(pipelineAggregatorFactory);
            return this;
        }

        /**
         * FOR TESTING ONLY
         */
        Builder skipResolveOrder() {
            this.skipResolveOrder = true;
            return this;
        }

        public AggregatorFactories build(AggregationContext context, AggregatorFactory<?> parent) throws IOException {
            if (aggregationBuilders.isEmpty() && pipelineAggregatorBuilders.isEmpty()) {
                return EMPTY;
            }
            List<PipelineAggregationBuilder> orderedpipelineAggregators = null;
            if (skipResolveOrder) {
                orderedpipelineAggregators = new ArrayList<>(pipelineAggregatorBuilders);
            } else {
                orderedpipelineAggregators = resolvePipelineAggregatorOrder(this.pipelineAggregatorBuilders, this.aggregationBuilders);
            }
            AggregatorFactory<?>[] aggFactories = new AggregatorFactory<?>[aggregationBuilders.size()];
            for (int i = 0; i < aggregationBuilders.size(); i++) {
                aggFactories[i] = aggregationBuilders.get(i).build(context, parent);
            }
            return new AggregatorFactories(parent, aggFactories, orderedpipelineAggregators);
        }

        private List<PipelineAggregationBuilder> resolvePipelineAggregatorOrder(
                List<PipelineAggregationBuilder> pipelineAggregatorBuilders, List<AggregationBuilder> aggBuilders) {
            Map<String, PipelineAggregationBuilder> pipelineAggregatorBuildersMap = new HashMap<>();
            for (PipelineAggregationBuilder builder : pipelineAggregatorBuilders) {
                pipelineAggregatorBuildersMap.put(builder.getName(), builder);
            }
            Map<String, AggregationBuilder> aggBuildersMap = new HashMap<>();
            for (AggregationBuilder aggBuilder : aggBuilders) {
                aggBuildersMap.put(aggBuilder.name, aggBuilder);
            }
            List<PipelineAggregationBuilder> orderedPipelineAggregatorrs = new LinkedList<>();
            List<PipelineAggregationBuilder> unmarkedBuilders = new ArrayList<PipelineAggregationBuilder>(pipelineAggregatorBuilders);
            Set<PipelineAggregationBuilder> temporarilyMarked = new HashSet<PipelineAggregationBuilder>();
            while (!unmarkedBuilders.isEmpty()) {
                PipelineAggregationBuilder builder = unmarkedBuilders.get(0);
                resolvePipelineAggregatorOrder(aggBuildersMap, pipelineAggregatorBuildersMap, orderedPipelineAggregatorrs, unmarkedBuilders,
                        temporarilyMarked, builder);
            }
            return orderedPipelineAggregatorrs;
        }

        private void resolvePipelineAggregatorOrder(Map<String, AggregationBuilder> aggBuildersMap,
                Map<String, PipelineAggregationBuilder> pipelineAggregatorBuildersMap,
                List<PipelineAggregationBuilder> orderedPipelineAggregators, List<PipelineAggregationBuilder> unmarkedBuilders,
                Set<PipelineAggregationBuilder> temporarilyMarked, PipelineAggregationBuilder builder) {
            if (temporarilyMarked.contains(builder)) {
                throw new IllegalArgumentException("Cyclical dependency found with pipeline aggregator [" + builder.getName() + "]");
            } else if (unmarkedBuilders.contains(builder)) {
                temporarilyMarked.add(builder);
                String[] bucketsPaths = builder.getBucketsPaths();
                for (String bucketsPath : bucketsPaths) {
                    List<AggregationPath.PathElement> bucketsPathElements = AggregationPath.parse(bucketsPath).getPathElements();
                    String firstAggName = bucketsPathElements.get(0).name;
                    if (bucketsPath.equals("_count") || bucketsPath.equals("_key")) {
                        continue;
                    } else if (aggBuildersMap.containsKey(firstAggName)) {
                        AggregationBuilder aggBuilder = aggBuildersMap.get(firstAggName);
                        for (int i = 1; i < bucketsPathElements.size(); i++) {
                            PathElement pathElement = bucketsPathElements.get(i);
                            String aggName = pathElement.name;
                            if ((i == bucketsPathElements.size() - 1) && (aggName.equalsIgnoreCase("_key") || aggName.equals("_count"))) {
                                break;
                            } else {
                                // Check the non-pipeline sub-aggregator
                                // factories
                                AggregationBuilder[] subBuilders = aggBuilder.factoriesBuilder.getAggregatorFactories();
                                boolean foundSubBuilder = false;
                                for (AggregationBuilder subBuilder : subBuilders) {
                                    if (aggName.equals(subBuilder.name)) {
                                        aggBuilder = subBuilder;
                                        foundSubBuilder = true;
                                        break;
                                    }
                                }
                                // Check the pipeline sub-aggregator factories
                                if (!foundSubBuilder && (i == bucketsPathElements.size() - 1)) {
                                    List<PipelineAggregationBuilder> subPipelineBuilders = aggBuilder.factoriesBuilder.pipelineAggregatorBuilders;
                                    for (PipelineAggregationBuilder subFactory : subPipelineBuilders) {
                                        if (aggName.equals(subFactory.getName())) {
                                            foundSubBuilder = true;
                                            break;
                                        }
                                    }
                                }
                                if (!foundSubBuilder) {
                                    throw new IllegalArgumentException("No aggregation [" + aggName + "] found for path [" + bucketsPath
                                            + "]");
                                }
                            }
                        }
                        continue;
                    } else {
                        PipelineAggregationBuilder matchingBuilder = pipelineAggregatorBuildersMap.get(firstAggName);
                        if (matchingBuilder != null) {
                            resolvePipelineAggregatorOrder(aggBuildersMap, pipelineAggregatorBuildersMap, orderedPipelineAggregators,
                                    unmarkedBuilders, temporarilyMarked, matchingBuilder);
                        } else {
                            throw new IllegalArgumentException("No aggregation found for path [" + bucketsPath + "]");
                        }
                    }
                }
                unmarkedBuilders.remove(builder);
                temporarilyMarked.remove(builder);
                orderedPipelineAggregators.add(builder);
            }
        }

        AggregationBuilder[] getAggregatorFactories() {
            return this.aggregationBuilders.toArray(new AggregationBuilder[this.aggregationBuilders.size()]);
        }

        List<PipelineAggregationBuilder> getPipelineAggregatorFactories() {
            return this.pipelineAggregatorBuilders;
        }

        public int count() {
            return aggregationBuilders.size() + pipelineAggregatorBuilders.size();
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            if (aggregationBuilders != null) {
                for (AggregationBuilder subAgg : aggregationBuilders) {
                    subAgg.toXContent(builder, params);
                }
            }
            if (pipelineAggregatorBuilders != null) {
                for (PipelineAggregationBuilder subAgg : pipelineAggregatorBuilders) {
                    subAgg.toXContent(builder, params);
                }
            }
            builder.endObject();
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(aggregationBuilders, pipelineAggregatorBuilders);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            Builder other = (Builder) obj;
            if (!Objects.equals(aggregationBuilders, other.aggregationBuilders))
                return false;
            if (!Objects.equals(pipelineAggregatorBuilders, other.pipelineAggregatorBuilders))
                return false;
            return true;
        }
    }
}
