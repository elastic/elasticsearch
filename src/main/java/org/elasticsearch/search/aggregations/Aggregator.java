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

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Scorer;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.aggregations.bucket.BucketsAggregator;
import org.elasticsearch.search.aggregations.bucket.DeferringBucketCollector;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.SearchContext.Lifetime;
import org.elasticsearch.search.query.QueryPhaseExecutionException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class Aggregator extends BucketCollector implements Releasable {

    /**
     * Returns whether one of the parents is a {@link BucketsAggregator}.
     */
    public static boolean hasParentBucketsAggregator(Aggregator parent) {
        while (parent != null) {
            if (parent instanceof BucketsAggregator) {
                return true;
            }
            parent = parent.parent;
        }
        return false;
    }

    private static final Predicate<Aggregator> COLLECTABLE_AGGREGATOR = new Predicate<Aggregator>() {
        @Override
        public boolean apply(Aggregator aggregator) {
            return aggregator.shouldCollect();
        }
    };

    public static final ParseField COLLECT_MODE = new ParseField("collect_mode");
    
    public enum SubAggCollectionMode {

        /**
         * Creates buckets and delegates to child aggregators in a single pass over
         * the matching documents
         */
        DEPTH_FIRST(new ParseField("depth_first")),

        /**
         * Creates buckets for all matching docs and then prunes to top-scoring buckets
         * before a second pass over the data when child aggregators are called
         * but only for docs from the top-scoring buckets
         */
        BREADTH_FIRST(new ParseField("breadth_first"));

        private final ParseField parseField;

        SubAggCollectionMode(ParseField parseField) {
            this.parseField = parseField;
        }

        public ParseField parseField() {
            return parseField;
        }

        public static SubAggCollectionMode parse(String value) {
            return parse(value, ParseField.EMPTY_FLAGS);
        }

        public static SubAggCollectionMode parse(String value, EnumSet<ParseField.Flag> flags) {
            SubAggCollectionMode[] modes = SubAggCollectionMode.values();
            for (SubAggCollectionMode mode : modes) {
                if (mode.parseField.match(value, flags)) {
                    return mode;
                }
            }
            throw new ElasticsearchParseException("No " + COLLECT_MODE.getPreferredName() + " found for value [" + value + "]");
        }
    }
    
    // A scorer used for the deferred collection mode to handle any child aggs asking for scores that are not 
    // recorded.
    static final Scorer unavailableScorer=new Scorer(null){
        private final String MSG = "A limitation of the " + SubAggCollectionMode.BREADTH_FIRST.parseField.getPreferredName()
                + " collection mode is that scores cannot be buffered along with document IDs";

        @Override
        public float score() throws IOException {
            throw new ElasticsearchParseException(MSG);
        }

        @Override
        public int freq() throws IOException {
            throw new ElasticsearchParseException(MSG);
        }

        @Override
        public int advance(int arg0) throws IOException {
            throw new ElasticsearchParseException(MSG);
        }

        @Override
        public long cost() {
            throw new ElasticsearchParseException(MSG);
        }

        @Override
        public int docID() {
            throw new ElasticsearchParseException(MSG);
        }

        @Override
        public int nextDoc() throws IOException {
            throw new ElasticsearchParseException(MSG);
        }};
    

    protected final String name;
    protected final Aggregator parent;
    protected final AggregationContext context;
    protected final BigArrays bigArrays;
    protected final int depth;
    private final Map<String, Object> metaData;

    protected final AggregatorFactories factories;
    protected final Aggregator[] subAggregators;
    protected BucketCollector collectableSubAggregators;

    private Map<String, Aggregator> subAggregatorbyName;
    private DeferringBucketCollector recordingWrapper;

    /**
     * Constructs a new Aggregator.
     *
     * @param name                  The name of the aggregation
     * @param factories             The factories for all the sub-aggregators under this aggregator
     * @param context               The aggregation context
     * @param parent                The parent aggregator (may be {@code null} for top level aggregators)
     * @param metaData              The metaData associated with this aggregator
     */
    protected Aggregator(String name, AggregatorFactories factories, AggregationContext context, Aggregator parent, Map<String, Object> metaData) throws IOException {
        this.name = name;
        this.metaData = metaData;
        this.parent = parent;
        this.context = context;
        this.bigArrays = context.bigArrays();
        this.depth = parent == null ? 0 : 1 + parent.depth();
        assert factories != null : "sub-factories provided to BucketAggregator must not be null, use AggragatorFactories.EMPTY instead";
        this.factories = factories;
        this.subAggregators = factories.createSubAggregators(this);
        context.searchContext().addReleasable(this, Lifetime.PHASE);
        // Register a safeguard to highlight any invalid construction logic (call to this constructor without subsequent preCollection call)
        collectableSubAggregators = new BucketCollector() {
            void badState(){
                throw new QueryPhaseExecutionException(Aggregator.this.context.searchContext(),
                        "preCollection not called on new Aggregator before use", null);                
            }
            @Override
            public void setNextReader(LeafReaderContext reader) {
                badState();
            }

            @Override
            public void preCollection() throws IOException {
                badState();
            }

            @Override
            public void postCollection() throws IOException {
                badState();
            }

            @Override
            public void collect(int docId, long bucketOrdinal) throws IOException {
                badState();
            }

            @Override
            public void gatherAnalysis(BucketAnalysisCollector results, long bucketOrdinal) {
                badState();
            }
        };
    }

    public Map<String, Object> getMetaData() {
        return this.metaData;
    }

    /**
     * Can be overriden by aggregator implementation to be called back when the collection phase starts.
     */
    protected void doPreCollection() throws IOException {
    }

    public final void preCollection() throws IOException {
        Iterable<Aggregator> collectables = Iterables.filter(Arrays.asList(subAggregators), COLLECTABLE_AGGREGATOR);
        List<BucketCollector> nextPassCollectors = new ArrayList<>();
        List<BucketCollector> thisPassCollectors = new ArrayList<>();
        for (Aggregator aggregator : collectables) {
            if (shouldDefer(aggregator)) {
                nextPassCollectors.add(aggregator);
            } else {
                thisPassCollectors.add(aggregator);
            }
        }
        if (nextPassCollectors.size() > 0) {
            BucketCollector deferreds = BucketCollector.wrap(nextPassCollectors);
            recordingWrapper = new DeferringBucketCollector(deferreds, context);
            // TODO. Without line below we are dependent on subclass aggs
            // delegating setNextReader calls on to child aggs
            // which they don't seem to do as a matter of course. Need to move
            // to a delegation model rather than broadcast
            context.registerReaderContextAware(recordingWrapper);
            thisPassCollectors.add(recordingWrapper);            
        }
        collectableSubAggregators = BucketCollector.wrap(thisPassCollectors);
        collectableSubAggregators.preCollection();
        doPreCollection();
    }
    
    /**
     * This method should be overidden by subclasses that want to defer calculation
     * of a child aggregation until a first pass is complete and a set of buckets has 
     * been pruned.
     * Deferring collection will require the recording of all doc/bucketIds from the first 
     * pass and then the sub class should call {@link #runDeferredCollections(long...)}  
     * for the selected set of buckets that survive the pruning.
     * @param aggregator the child aggregator 
     * @return true if the aggregator should be deferred
     * until a first pass at collection has completed
     */
    protected boolean shouldDefer(Aggregator aggregator) {
        return false;
    }
    
    protected void runDeferredCollections(long... bucketOrds){
        // Being lenient here - ignore calls where there are no deferred collections to playback
        if (recordingWrapper != null) {
            context.setScorer(unavailableScorer);
            recordingWrapper.prepareSelectedBuckets(bucketOrds);
        } 
    }

    /**
     * @return  The name of the aggregation.
     */
    public String name() {
        return name;
    }

    /** Return the depth of this aggregator in the aggregation tree. */
    public final int depth() {
        return depth;
    }

    /**
     * @return  The parent aggregator of this aggregator. The addAggregation are hierarchical in the sense that some can
     *          be composed out of others (more specifically, bucket addAggregation can define other addAggregation that will
     *          be aggregated per bucket). This method returns the direct parent aggregator that contains this aggregator, or
     *          {@code null} if there is none (meaning, this aggregator is a top level one)
     */
    public Aggregator parent() {
        return parent;
    }

    public Aggregator[] subAggregators() {
        return subAggregators;
    }

    public Aggregator subAggregator(String aggName) {
        if (subAggregatorbyName == null) {
            subAggregatorbyName = new HashMap<>(subAggregators.length);
            for (int i = 0; i < subAggregators.length; i++) {
                subAggregatorbyName.put(subAggregators[i].name, subAggregators[i]);
            }
        }
        return subAggregatorbyName.get(aggName);
    }

    /**
     * @return  The current aggregation context.
     */
    public AggregationContext context() {
        return context;
    }

    /**
     * @return  Whether this aggregator is in the state where it can collect documents. Some aggregators can do their aggregations without
     *          actually collecting documents, for example, an aggregator that computes stats over unmapped fields doesn't need to collect
     *          anything as it knows to just return "empty" stats as the aggregation result.
     */
    public abstract boolean shouldCollect();

    /**
     * Called after collection of all document is done.
     */
    public final void postCollection() throws IOException {
        collectableSubAggregators.postCollection();
        doPostCollection();
    }

    /** Called upon release of the aggregator. */
    @Override
    public void close() {
        try (Releasable _ = recordingWrapper) {
            doClose();
        }
    }

    /** Release instance-specific data. */
    protected void doClose() {}

    /**
     * Can be overriden by aggregator implementation to be called back when the collection phase ends.
     */
    protected void doPostCollection() throws IOException {
    }

    /**
     * @return  The aggregated & built aggregation
     */
    public abstract InternalAggregation buildAggregation(long owningBucketOrdinal) throws IOException;
    
    @Override
    public void gatherAnalysis(BucketAnalysisCollector results, long bucketOrdinal) throws IOException {
        results.add(buildAggregation(bucketOrdinal));
    }
    
    public abstract InternalAggregation buildEmptyAggregation();

    protected final InternalAggregations buildEmptySubAggregations() {
        List<InternalAggregation> aggs = new ArrayList<>();
        for (Aggregator aggregator : subAggregators) {
            aggs.add(aggregator.buildEmptyAggregation());
        }
        return new InternalAggregations(aggs);
    }

    /**
     * Parses the aggregation request and creates the appropriate aggregator factory for it.
     *
     * @see {@link AggregatorFactory}
    */
    public static interface Parser {

        /**
         * @return The aggregation type this parser is associated with.
         */
        String type();

        /**
         * Returns the aggregator factory with which this parser is associated, may return {@code null} indicating the
         * aggregation should be skipped (e.g. when trying to aggregate on unmapped fields).
         *
         * @param aggregationName   The name of the aggregation
         * @param parser            The xcontent parser
         * @param context           The search context
         * @return                  The resolved aggregator factory or {@code null} in case the aggregation should be skipped
         * @throws java.io.IOException      When parsing fails
         */
        AggregatorFactory parse(String aggregationName, XContentParser parser, SearchContext context) throws IOException;

    }

}
