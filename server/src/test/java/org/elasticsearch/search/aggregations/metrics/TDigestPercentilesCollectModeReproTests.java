/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.metrics;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.Aggregator.SubAggCollectionMode;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;

import java.io.IOException;
import java.lang.reflect.Field;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * Reproduction for the OOM investigation on {@code terms(destination.port) -> percentiles(destination.port)}.
 * <p>
 * A {@link org.elasticsearch.search.aggregations.metrics.HistogramUnionState} (T-Digest sketch) is allocated per
 * <em>collected</em> bucket. The question the heap dump raised was: does the sub-agg collect one sketch per distinct
 * value (depth-first, ~cardinality sketches — the 500&nbsp;MB path) or only for the surviving {@code shardSize}
 * buckets (breadth-first)? This test drives both modes over a high-cardinality numeric field and reads the live
 * aggregator's {@code states} array to show the difference deterministically.
 */
public class TDigestPercentilesCollectModeReproTests extends AggregatorTestCase {

    private static final int CARDINALITY = 30_000;
    private static final String FIELD = "port";

    public void testDepthFirstAllocatesOneSketchPerDistinctValue() throws IOException {
        long[] observed = runWithCollectMode(SubAggCollectionMode.DEPTH_FIRST);
        long statesLen = observed[0];
        long nonNull = observed[1];
        logger.info("DEPTH_FIRST -> states.length={}, live sketches={}, sketch ramBytes={}", statesLen, nonNull, observed[2]);
        // Depth-first collects the sub-agg inline for every distinct port, so one live sketch per distinct value.
        assertThat("depth-first should build ~one sketch per distinct value", nonNull, greaterThanOrEqualTo((long) CARDINALITY));
    }

    public void testBreadthFirstAllocatesOnlyShardSizeSketches() throws IOException {
        long[] observed = runWithCollectMode(SubAggCollectionMode.BREADTH_FIRST);
        long nonNull = observed[1];
        logger.info("BREADTH_FIRST -> states.length={}, live sketches={}, sketch ramBytes={}", observed[0], nonNull, observed[2]);
        // Breadth-first defers and replays only the selected top buckets, so the sub-agg builds ~shardSize sketches.
        assertThat("breadth-first should build far fewer than the cardinality", nonNull, lessThanOrEqualTo(100L));
    }

    public void testDefaultHeuristicIsBreadthFirstForNumericField() throws IOException {
        // No explicit collect_mode: exercises TermsAggregatorFactory.pickSubAggColectMode. For a numeric field
        // (maxOrd == -1) with a bounded shardSize and a sub-agg, the heuristic must pick breadth-first (cheap).
        long[] observed = runWithCollectMode(null);
        long nonNull = observed[1];
        logger.info("DEFAULT -> states.length={}, live sketches={}, sketch ramBytes={}", observed[0], nonNull, observed[2]);
        assertThat("default heuristic should behave like breadth-first for a numeric field", nonNull, lessThanOrEqualTo(100L));
    }

    /**
     * @return {@code [states.length, liveSketchCount, summedSketchRamBytes]} read off the live percentiles aggregator
     *         after collection.
     */
    private long[] runWithCollectMode(SubAggCollectionMode collectMode) throws IOException {
        PercentilesAggregationBuilder pct = new PercentilesAggregationBuilder("pct").field(FIELD)
            .percentilesConfig(new PercentilesConfig.TDigest());
        TermsAggregationBuilder terms = new TermsAggregationBuilder("ports").field(FIELD).size(9).shardSize(25).subAggregation(pct);
        if (collectMode != null) {
            terms.collectMode(collectMode);
        }

        NumberFieldMapper.NumberFieldType fieldType = new NumberFieldMapper.NumberFieldType(FIELD, NumberFieldMapper.NumberType.LONG);

        long[] result = new long[3];
        testCase(iw -> {
            for (int i = 0; i < CARDINALITY; i++) {
                Document doc = new Document();
                doc.add(new SortedNumericDocValuesField(FIELD, i)); // one distinct port value per doc
                iw.addDocument(doc);
            }
            iw.forceMerge(1); // single segment -> single leaf -> single aggregator, so checkAggregator sees all docs
        },
            (InternalAggregation ignored) -> {},
            // split=true routes through the branch that inspects the live aggregator BEFORE releaseAggregations()
            new AggTestConfig(terms, fieldType).withSplitLeavesIntoSeperateAggregators(true).withCheckAggregator(agg -> {
                AbstractTDigestPercentilesAggregator pctAgg = findPercentilesAggregator(agg);
                assertNotNull("could not locate the percentiles aggregator in the tree", pctAgg);
                long len = pctAgg.states.size();
                long live = 0;
                long ram = 0;
                for (long i = 0; i < len; i++) {
                    HistogramUnionState state = pctAgg.states.get(i);
                    if (state != null) {
                        live++;
                        ram += state.ramBytesUsed();
                    }
                }
                result[0] = len;
                result[1] = live;
                result[2] = ram;
            })
        );
        return result;
    }

    /**
     * Walks the aggregator tree to find the {@link AbstractTDigestPercentilesAggregator}. Under breadth-first the
     * sub-agg is wrapped in a {@code DeferringBucketCollector.WrappedAggregator} whose delegate is the private
     * {@code in} field, so unwrap that reflectively when present.
     */
    private static AbstractTDigestPercentilesAggregator findPercentilesAggregator(Aggregator agg) {
        if (agg instanceof AbstractTDigestPercentilesAggregator found) {
            return found;
        }
        Aggregator unwrapped = tryUnwrap(agg);
        if (unwrapped != null) {
            AbstractTDigestPercentilesAggregator r = findPercentilesAggregator(unwrapped);
            if (r != null) {
                return r;
            }
        }
        for (Aggregator sub : agg.subAggregators()) {
            AbstractTDigestPercentilesAggregator r = findPercentilesAggregator(sub);
            if (r != null) {
                return r;
            }
        }
        return null;
    }

    private static Aggregator tryUnwrap(Aggregator agg) {
        // The deferring wrapper holds the real sub-agg in an Aggregator-typed field; find it without assuming a name.
        for (Class<?> c = agg.getClass(); c != null && c != Object.class; c = c.getSuperclass()) {
            for (Field f : c.getDeclaredFields()) {
                if (Aggregator.class.isAssignableFrom(f.getType())) {
                    try {
                        f.setAccessible(true);
                        Object delegate = f.get(agg);
                        if (delegate instanceof Aggregator a && a != agg) {
                            return a;
                        }
                    } catch (IllegalAccessException e) {
                        // skip fields we cannot read
                    }
                }
            }
        }
        return null;
    }
}
