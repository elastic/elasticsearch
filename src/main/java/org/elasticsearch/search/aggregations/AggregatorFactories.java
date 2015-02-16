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

import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.search.aggregations.reducers.Reducer;
import org.elasticsearch.search.aggregations.reducers.ReducerFactory;
import org.elasticsearch.search.aggregations.support.AggregationContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *
 */
public class AggregatorFactories {

    public static final AggregatorFactories EMPTY = new Empty();

    private AggregatorFactory[] factories;
    private List<ReducerFactory> reducerFactories;

    public static Builder builder() {
        return new Builder();
    }

    private AggregatorFactories(AggregatorFactory[] factories, List<ReducerFactory> reducers) {
        this.factories = factories;
        this.reducerFactories = reducers;
    }

    public List<Reducer> createReducers() throws IOException {
        List<Reducer> reducers = new ArrayList<>();
        for (ReducerFactory factory : this.reducerFactories) {
            reducers.add(factory.create(null, null, false)); // NOCOMIT add context, parent etc.
        }
        return reducers;
    }

    private static Aggregator createAndRegisterContextAware(AggregationContext context, AggregatorFactory factory, Aggregator parent, boolean collectsFromSingleBucket) throws IOException {
        final Aggregator aggregator = factory.create(context, parent, collectsFromSingleBucket);
        // Once the aggregator is fully constructed perform any initialisation -
        // can't do everything in constructors if Aggregator base class needs
        // to delegate to subclasses as part of construction.
        aggregator.preCollection();
        return aggregator;
    }

    /**
     * Create all aggregators so that they can be consumed with multiple buckets.
     */
    public Aggregator[] createSubAggregators(Aggregator parent) throws IOException {
        Aggregator[] aggregators = new Aggregator[count()];
        for (int i = 0; i < factories.length; ++i) {
            // TODO: sometimes even sub aggregations always get called with bucket 0, eg. if
            // you have a terms agg under a top-level filter agg. We should have a way to
            // propagate the fact that only bucket 0 will be collected with single-bucket
            // aggs
            final boolean collectsFromSingleBucket = false;
            aggregators[i] = createAndRegisterContextAware(parent.context(), factories[i], parent, collectsFromSingleBucket);
        }
        return aggregators;
    }

    public Aggregator[] createTopLevelAggregators(AggregationContext ctx) throws IOException {
        // These aggregators are going to be used with a single bucket ordinal, no need to wrap the PER_BUCKET ones
        Aggregator[] aggregators = new Aggregator[factories.length];
        for (int i = 0; i < factories.length; i++) {
            // top-level aggs only get called with bucket 0
            final boolean collectsFromSingleBucket = true;
            aggregators[i] = createAndRegisterContextAware(ctx, factories[i], null, collectsFromSingleBucket);
        }
        return aggregators;
    }

    public int count() {
        return factories.length;
    }

    void setParent(AggregatorFactory parent) {
        for (AggregatorFactory factory : factories) {
            factory.parent = parent;
        }
    }

    public void validate() {
        for (AggregatorFactory factory : factories) {
            factory.validate();
        }
        for (ReducerFactory factory : reducerFactories) {
            factory.validate();
        }
    }

    private final static class Empty extends AggregatorFactories {

        private static final AggregatorFactory[] EMPTY_FACTORIES = new AggregatorFactory[0];
        private static final Aggregator[] EMPTY_AGGREGATORS = new Aggregator[0];
        private static final List<ReducerFactory> EMPTY_REDUCERS = new ArrayList<>();

        private Empty() {
            super(EMPTY_FACTORIES, EMPTY_REDUCERS);
        }

        @Override
        public Aggregator[] createSubAggregators(Aggregator parent) {
            return EMPTY_AGGREGATORS;
        }

        @Override
        public Aggregator[] createTopLevelAggregators(AggregationContext ctx) {
            return EMPTY_AGGREGATORS;
        }

    }

    public static class Builder {

        private final Set<String> names = new HashSet<>();
        private final List<AggregatorFactory> factories = new ArrayList<>();
        private final List<ReducerFactory> reducerFactories = new ArrayList<>();

        public Builder addAggregator(AggregatorFactory factory) {
            if (!names.add(factory.name)) {
                throw new ElasticsearchIllegalArgumentException("Two sibling aggregations cannot have the same name: [" + factory.name + "]");
            }
            factories.add(factory);
            return this;
        }

        public Builder addReducer(ReducerFactory reducerFactory) {
            this.reducerFactories.add(reducerFactory);
            return this;
        }

        public AggregatorFactories build() {
            if (factories.isEmpty()) {
                return EMPTY;
            }
            List<ReducerFactory> orderedReducers = resolveReducerOrder(this.reducerFactories, this.factories);
            return new AggregatorFactories(factories.toArray(new AggregatorFactory[factories.size()]), orderedReducers);
        }

        /*
         * L ← Empty list that will contain the sorted nodes
         * while there are unmarked nodes do
         *     select an unmarked node n
         *     visit(n) 
         * function visit(node n)
         *     if n has a temporary mark then stop (not a DAG)
         *     if n is not marked (i.e. has not been visited yet) then
         *         mark n temporarily
         *         for each node m with an edge from n to m do
         *             visit(m)
         *         mark n permanently
         *         unmark n temporarily
         *         add n to head of L
         */
        private List<ReducerFactory> resolveReducerOrder(List<ReducerFactory> reducerFactories, List<AggregatorFactory> aggFactories) {
            Map<String, ReducerFactory> reducerFactoriesMap = new HashMap<>();
            for (ReducerFactory factory : reducerFactories) {
                reducerFactoriesMap.put(factory.getName(), factory);
            }
            Set<String> aggFactoryNames = new HashSet<>();
            for (AggregatorFactory aggFactory : aggFactories) {
                aggFactoryNames.add(aggFactory.name);
            }
            List<ReducerFactory> orderedReducers = new LinkedList<>();
            List<ReducerFactory> unmarkedFactories = new ArrayList<ReducerFactory>(reducerFactories);
            Set<ReducerFactory> temporarilyMarked = new HashSet<ReducerFactory>();
            while (!unmarkedFactories.isEmpty()) {
                ReducerFactory factory = unmarkedFactories.get(0);
                resolveReducerOrder(aggFactoryNames, reducerFactoriesMap, orderedReducers, unmarkedFactories, temporarilyMarked, factory);
            }
            List<String> orderedReducerNames = new ArrayList<>();
            for (ReducerFactory reducerFactory : orderedReducers) {
                orderedReducerNames.add(reducerFactory.getName());
            }
            return orderedReducers;
        }

        private void resolveReducerOrder(Set<String> aggFactoryNames, Map<String, ReducerFactory> reducerFactoriesMap,
                List<ReducerFactory> orderedReducers, List<ReducerFactory> unmarkedFactories, Set<ReducerFactory> temporarilyMarked,
                ReducerFactory factory) {
            if (temporarilyMarked.contains(factory)) {
                throw new ElasticsearchIllegalStateException("Cyclical dependancy found with reducer [" + factory.getName() + "]"); // NOCOMMIT is this the right Exception to throw?
            } else if (unmarkedFactories.contains(factory)) {
                temporarilyMarked.add(factory);
                String[] bucketsPaths = factory.getBucketsPaths();
                for (String bucketsPath : bucketsPaths) {
                    ReducerFactory matchingFactory = reducerFactoriesMap.get(bucketsPath);
                    if (aggFactoryNames.contains(bucketsPath)) {
                        continue;
                    } else if (matchingFactory != null) {
                        resolveReducerOrder(aggFactoryNames, reducerFactoriesMap, orderedReducers, unmarkedFactories, temporarilyMarked,
                                matchingFactory);
                    } else {
                        throw new ElasticsearchIllegalStateException("No reducer found for path [" + bucketsPath + "]"); // NOCOMMIT is this the right Exception to throw?
                    }
                }
                unmarkedFactories.remove(factory);
                temporarilyMarked.remove(factory);
                orderedReducers.add(factory);
            }
        }
    }
}
