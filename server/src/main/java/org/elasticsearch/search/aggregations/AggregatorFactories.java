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

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.NamedObjectNotFoundException;
import org.elasticsearch.common.xcontent.SuggestingErrorOnUnknown;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentLocation;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.Rewriteable;
import org.elasticsearch.search.aggregations.bucket.global.GlobalAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator.PipelineTree;
import org.elasticsearch.search.aggregations.support.AggregationPath;
import org.elasticsearch.search.aggregations.support.AggregationPath.PathElement;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.profile.Profilers;
import org.elasticsearch.search.profile.aggregation.ProfilingAggregator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

/**
 * An immutable collection of {@link AggregatorFactories}.
 */
public class AggregatorFactories {
    public static final Pattern VALID_AGG_NAME = Pattern.compile("[^\\[\\]>]+");

    /**
     * Parses the aggregation request recursively generating aggregator
     * factories in turn.
     */
    public static AggregatorFactories.Builder parseAggregators(XContentParser parser) throws IOException {
        return parseAggregators(parser, 0);
    }

    private static AggregatorFactories.Builder parseAggregators(XContentParser parser, int level) throws IOException {
        Matcher validAggMatcher = VALID_AGG_NAME.matcher("");
        AggregatorFactories.Builder factories = new AggregatorFactories.Builder();

        XContentParser.Token token = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token != XContentParser.Token.FIELD_NAME) {
                throw new ParsingException(parser.getTokenLocation(),
                        "Unexpected token " + token + " in [aggs]: aggregations definitions must start with the name of the aggregation.");
            }
            final String aggregationName = parser.currentName();
            if (!validAggMatcher.reset(aggregationName).matches()) {
                throw new ParsingException(parser.getTokenLocation(), "Invalid aggregation name [" + aggregationName
                        + "]. Aggregation names can contain any character except '[', ']', and '>'");
            }

            token = parser.nextToken();
            if (token != XContentParser.Token.START_OBJECT) {
                throw new ParsingException(parser.getTokenLocation(), "Aggregation definition for [" + aggregationName + " starts with a ["
                        + token + "], expected a [" + XContentParser.Token.START_OBJECT + "].");
            }

            BaseAggregationBuilder aggBuilder = null;
            AggregatorFactories.Builder subFactories = null;

            Map<String, Object> metadata = null;

            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token != XContentParser.Token.FIELD_NAME) {
                    throw new ParsingException(
                            parser.getTokenLocation(), "Expected [" + XContentParser.Token.FIELD_NAME + "] under a ["
                            + XContentParser.Token.START_OBJECT + "], but got a [" + token + "] in [" + aggregationName + "]",
                            parser.getTokenLocation());
                }
                final String fieldName = parser.currentName();

                token = parser.nextToken();
                if (token == XContentParser.Token.START_OBJECT) {
                    switch (fieldName) {
                    case "meta":
                        metadata = parser.map();
                        break;
                    case "aggregations":
                    case "aggs":
                        if (subFactories != null) {
                            throw new ParsingException(parser.getTokenLocation(),
                                    "Found two sub aggregation definitions under [" + aggregationName + "]");
                        }
                        subFactories = parseAggregators(parser, level + 1);
                        break;
                    default:
                        if (aggBuilder != null) {
                            throw new ParsingException(parser.getTokenLocation(), "Found two aggregation type definitions in ["
                                    + aggregationName + "]: [" + aggBuilder.getType() + "] and [" + fieldName + "]");
                        }

                        try {
                            aggBuilder = parser.namedObject(BaseAggregationBuilder.class, fieldName, aggregationName);
                        } catch (NamedObjectNotFoundException ex) {
                            String message = String.format(Locale.ROOT, "Unknown aggregation type [%s]%s", fieldName,
                                SuggestingErrorOnUnknown.suggest(fieldName, ex.getCandidates()));
                            throw new ParsingException(new XContentLocation(ex.getLineNumber(), ex.getColumnNumber()), message, ex);
                        }
                    }
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "Expected [" + XContentParser.Token.START_OBJECT + "] under ["
                            + fieldName + "], but got a [" + token + "] in [" + aggregationName + "]");
                }
            }

            if (aggBuilder == null) {
                throw new ParsingException(parser.getTokenLocation(), "Missing definition for aggregation [" + aggregationName + "]",
                        parser.getTokenLocation());
            } else {
                if (metadata != null) {
                    aggBuilder.setMetadata(metadata);
                }

                if (subFactories != null) {
                    aggBuilder.subAggregations(subFactories);
                }

                if (aggBuilder instanceof AggregationBuilder) {
                    factories.addAggregator((AggregationBuilder) aggBuilder);
                } else {
                    factories.addPipelineAggregator((PipelineAggregationBuilder) aggBuilder);
                }
            }
        }

        return factories;
    }

    public static final AggregatorFactories EMPTY = new AggregatorFactories(new AggregatorFactory[0]);

    private AggregatorFactory[] factories;

    public static Builder builder() {
        return new Builder();
    }

    private AggregatorFactories(AggregatorFactory[] factories) {
        this.factories = factories;
    }

    /**
     * Create all aggregators so that they can be consumed with multiple
     * buckets.
     * @param cardinality Upper bound of the number of {@code owningBucketOrd}s
     *                    that {@link Aggregator}s created by this method will
     *                    be asked to collect.
     */
    public Aggregator[] createSubAggregators(SearchContext searchContext, Aggregator parent, CardinalityUpperBound cardinality)
                throws IOException {
        Aggregator[] aggregators = new Aggregator[countAggregators()];
        for (int i = 0; i < factories.length; ++i) {
            Aggregator factory = factories[i].create(searchContext, parent, cardinality);
            Profilers profilers = factory.context().getProfilers();
            if (profilers != null) {
                factory = new ProfilingAggregator(factory, profilers.getAggregationProfiler());
            }
            aggregators[i] = factory;
        }
        return aggregators;
    }

    public Aggregator[] createTopLevelAggregators(SearchContext searchContext) throws IOException {
        // These aggregators are going to be used with a single bucket ordinal, no need to wrap the PER_BUCKET ones
        Aggregator[] aggregators = new Aggregator[factories.length];
        for (int i = 0; i < factories.length; i++) {
            /*
             * Top level aggs only collect from owningBucketOrd 0 which is
             * *exactly* what CardinalityUpperBound.ONE *means*.  
             */
            Aggregator factory = factories[i].create(searchContext, null, CardinalityUpperBound.ONE);
            Profilers profilers = factory.context().getProfilers();
            if (profilers != null) {
                factory = new ProfilingAggregator(factory, profilers.getAggregationProfiler());
            }
            aggregators[i] = factory;
        }
        return aggregators;
    }

    /**
     * @return the number of sub-aggregator factories
     */
    public int countAggregators() {
        return factories.length;
    }

    /**
     * A mutable collection of {@link AggregationBuilder}s and
     * {@link PipelineAggregationBuilder}s.
     */
    public static class Builder implements Writeable, ToXContentObject {
        private final Set<String> names = new HashSet<>();

        // Using LinkedHashSets to preserve the order of insertion, that makes the results
        // ordered nicely, although technically order does not matter
        private final Collection<AggregationBuilder> aggregationBuilders = new LinkedHashSet<>();
        private final Collection<PipelineAggregationBuilder> pipelineAggregatorBuilders = new LinkedHashSet<>();

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

        public boolean mustVisitAllDocs() {
            for (AggregationBuilder builder : aggregationBuilders) {
                if (builder instanceof GlobalAggregationBuilder) {
                    return true;
                } else if (builder instanceof TermsAggregationBuilder) {
                    if (((TermsAggregationBuilder) builder).minDocCount() == 0) {
                        return true;
                    }
                }

            }
            return false;
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
         * Validate the root of the aggregation tree.
         */
        public ActionRequestValidationException validate(ActionRequestValidationException e) {
            PipelineAggregationBuilder.ValidationContext context =
                    PipelineAggregationBuilder.ValidationContext.forTreeRoot(aggregationBuilders, pipelineAggregatorBuilders, e);
            validatePipelines(context);
            return validateChildren(context.getValidationException());
        }

        /**
         * Validate a the pipeline aggregations in this factory.
         */
        private void validatePipelines(PipelineAggregationBuilder.ValidationContext context) {
            List<PipelineAggregationBuilder> orderedPipelineAggregators;
            try {
                orderedPipelineAggregators = resolvePipelineAggregatorOrder(pipelineAggregatorBuilders, aggregationBuilders);
            } catch (IllegalArgumentException iae) {
                context.addValidationError(iae.getMessage());
                return;
            }
            for (PipelineAggregationBuilder builder : orderedPipelineAggregators) {
                builder.validate(context);
            }
        }

        /**
         * Validate a the children of this factory.
         */
        private ActionRequestValidationException validateChildren(ActionRequestValidationException e) {
            for (AggregationBuilder agg : aggregationBuilders) {
                PipelineAggregationBuilder.ValidationContext context =
                        PipelineAggregationBuilder.ValidationContext.forInsideTree(agg, e);
                agg.factoriesBuilder.validatePipelines(context);
                e = agg.factoriesBuilder.validateChildren(context.getValidationException());
            }
            return e;
        }

        public AggregatorFactories build(QueryShardContext queryShardContext, AggregatorFactory parent) throws IOException {
            if (aggregationBuilders.isEmpty() && pipelineAggregatorBuilders.isEmpty()) {
                return EMPTY;
            }
            AggregatorFactory[] aggFactories = new AggregatorFactory[aggregationBuilders.size()];
            int i = 0;
            for (AggregationBuilder agg : aggregationBuilders) {
                aggFactories[i] = agg.build(queryShardContext, parent);
                ++i;
            }
            return new AggregatorFactories(aggFactories);
        }

        private List<PipelineAggregationBuilder> resolvePipelineAggregatorOrder(
                Collection<PipelineAggregationBuilder> pipelineAggregatorBuilders, Collection<AggregationBuilder> aggregationBuilders) {
            Map<String, PipelineAggregationBuilder> pipelineAggregatorBuildersMap = new HashMap<>();
            for (PipelineAggregationBuilder builder : pipelineAggregatorBuilders) {
                pipelineAggregatorBuildersMap.put(builder.getName(), builder);
            }
            Map<String, AggregationBuilder> aggBuildersMap = new HashMap<>();
            for (AggregationBuilder aggBuilder : aggregationBuilders) {
                aggBuildersMap.put(aggBuilder.name, aggBuilder);
            }
            List<PipelineAggregationBuilder> orderedPipelineAggregatorrs = new LinkedList<>();
            List<PipelineAggregationBuilder> unmarkedBuilders = new ArrayList<>(pipelineAggregatorBuilders);
            Collection<PipelineAggregationBuilder> temporarilyMarked = new HashSet<>();
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
                Collection<PipelineAggregationBuilder> temporarilyMarked, PipelineAggregationBuilder builder) {
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
                                Collection<AggregationBuilder> subBuilders = aggBuilder.factoriesBuilder.aggregationBuilders;
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
                                    Collection<PipelineAggregationBuilder> subPipelineBuilders = aggBuilder.factoriesBuilder
                                            .pipelineAggregatorBuilders;
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

        public Collection<AggregationBuilder> getAggregatorFactories() {
            return Collections.unmodifiableCollection(aggregationBuilders);
        }

        public Collection<PipelineAggregationBuilder> getPipelineAggregatorFactories() {
            return Collections.unmodifiableCollection(pipelineAggregatorBuilders);
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
        public String toString() {
            return Strings.toString(this, true, true);
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

        /**
         * Rewrites the underlying aggregation builders into their primitive
         * form. If the builder did not change the identity reference must be
         * returned otherwise the builder will be rewritten infinitely.
         */
        public Builder rewrite(QueryRewriteContext context) throws IOException {
            boolean changed = false;
            Builder newBuilder = new Builder();

            for (AggregationBuilder builder : aggregationBuilders) {
                AggregationBuilder result = Rewriteable.rewrite(builder, context);
                newBuilder.addAggregator(result);
                changed |= result != builder;
            }
            for (PipelineAggregationBuilder builder : pipelineAggregatorBuilders) {
                PipelineAggregationBuilder result = Rewriteable.rewrite(builder, context);
                newBuilder.addPipelineAggregator(result);
                changed |= result != builder;
            }

            return changed ? newBuilder : this;
        }

        /**
         * Build a tree of {@link PipelineAggregator}s to modify the tree of
         * aggregation results after the final reduction.
         */
        public PipelineTree buildPipelineTree() {
            if (aggregationBuilders.isEmpty() && pipelineAggregatorBuilders.isEmpty()) {
                return PipelineTree.EMPTY;
            }
            Map<String, PipelineTree> subTrees = aggregationBuilders.stream()
                    .collect(toMap(AggregationBuilder::getName, AggregationBuilder::buildPipelineTree));
            List<PipelineAggregator> aggregators = resolvePipelineAggregatorOrder(pipelineAggregatorBuilders, aggregationBuilders)
                    .stream()
                    .map(PipelineAggregationBuilder::create)
                    .collect(toList());
            return new PipelineTree(subTrees, aggregators);
        }
    }
}
