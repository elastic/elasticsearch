/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.DelayableWriteable;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.lucene.search.TopDocsAndMaxScore;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.metrics.InternalTopHits;
import org.elasticsearch.search.lookup.Source;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

/**
 * Regression coverage for pooled {@link SearchHits} lifecycle when {@link QueryPhaseResultConsumer.MergeResult}
 * serializes {@link DelayableWriteable}{@code <InternalAggregations>} across differing transport versions
 * (wire compatibility rewrite inside {@link DelayableWriteable.Serialized#writeTo}).
 */
public class MergeResultWireCompatibilityTopHitsTests extends ESTestCase {

    private static final TransportVersion BATCHED_QUERY_EXECUTION_DELAYABLE_WRITEABLE = TransportVersion.fromName(
        "batched_query_execution_delayable_writeable"
    );

    @Override
    protected NamedWriteableRegistry writableRegistry() {
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>(new SearchModule(Settings.EMPTY, emptyList()).getNamedWriteables());
        return new NamedWriteableRegistry(entries);
    }

    /**
     * Regression test: pooled {@link SearchHits} created transiently inside
     * {@link DelayableWriteable.Serialized#writeTo} (the version-mismatch branch that calls
     * {@code referencing(expand()).writeTo(out)}) must be released after serialization completes.
     *
     * <p>The test intercepts the {@link InternalAggregations} reader used by {@code expand()} and
     * takes an extra reference on each {@link SearchHits} via
     * {@link InternalAggregations#addTopHitsToReleaseList} with {@code takeRef=true}.  After
     * {@link QueryPhaseResultConsumer.MergeResult#writeTo} returns the transient hits must have
     * refcount&nbsp;==&nbsp;1 (only our extra ref remains, meaning the original was released).
     * Without the fix the original ref is never released and the refcount stays at 2.
     */
    public void testMergeResultWriteToDoesNotLeakPooledTopHitsOnSerializedVersionMismatch() throws Exception {
        TransportVersion serializedVersion = TransportVersion.current();
        TransportVersion outVersion = TransportVersionUtils.getPreviousVersion(serializedVersion);
        assumeFalse("Test requires two distinct wire versions", outVersion.equals(serializedVersion));
        assumeTrue(
            "MergeResult uses DelayableWriteable path only when BATCHED_QUERY_EXECUTION_DELAYABLE_WRITEABLE is supported on out",
            outVersion.supports(BATCHED_QUERY_EXECUTION_DELAYABLE_WRITEABLE)
        );

        InternalAggregations pooledAggs = roundTripToPooledTopHits(createUnpooledAggregationsWithTopHits(), serializedVersion);
        SearchHits pooledHits = pooledAggs.<InternalTopHits>get("th").getHits();
        assertTrue("round-trip must yield pooled SearchHits (hit with _source)", pooledHits.isPooled());

        // Capture the SearchHits that expand() will materialise inside Serialized.writeTo().
        // takeRef=true: we take an extra reference so we can inspect the refcount after writeTo.
        List<SearchHits> transientHits = new ArrayList<>();
        Writeable.Reader<InternalAggregations> capturingReader = in -> {
            InternalAggregations aggs = InternalAggregations.readFrom(in);
            InternalAggregations.addTopHitsToReleaseList(aggs, transientHits, true);
            return aggs;
        };

        try (
            DelayableWriteable<InternalAggregations> serialized = DelayableWriteable.referencing(pooledAggs)
                .asSerialized(capturingReader, writableRegistry())
        ) {
            assertTrue(serialized.isSerialized());

            QueryPhaseResultConsumer.MergeResult mergeResult = new QueryPhaseResultConsumer.MergeResult(List.of(), null, serialized, 0L);

            BytesStreamOutput out = new BytesStreamOutput();
            out.setTransportVersion(outVersion);
            mergeResult.writeTo(out);

            assertFalse("version mismatch must have triggered expand() — no transient hits were captured", transientHits.isEmpty());

            // Without fix: decRef returns false (original ref leaked; our extra ref was not the last).
            // With fix: decRef returns true (original was released; our extra ref is the last → count hits 0).
            // Remove before decRef to prevent double-release in the finally block.
            for (var it = transientHits.iterator(); it.hasNext();) {
                SearchHits h = it.next();
                it.remove();
                assertTrue(
                    "transient SearchHits must be released after Serialized wire rewrite (expand + re-serialize)",
                    h.decRef()
                );
            }
        } finally {
            // Release any extra refs not yet consumed above (e.g. when writeTo threw before the assertion).
            for (SearchHits h : transientHits) {
                h.decRef();
            }
            pooledHits.decRef();
        }
    }

    /**
     * Regression test for Site B: pooled {@link SearchHits} expanded via {@code reducedAggs.expand()} in the
     * pre-{@code BATCHED_QUERY_EXECUTION_DELAYABLE_WRITEABLE} fallback path of
     * {@link QueryPhaseResultConsumer.MergeResult#writeTo} must be released after writing.
     *
     * <p>Uses the same {@code capturingReader} / {@code takeRef=true} / refcount-delta technique as the
     * Site A test: we take an extra reference on each captured {@link SearchHits} before {@code writeTo},
     * then assert refcount&nbsp;==&nbsp;1 afterwards (only our ref remains — the original was released).
     * Without the fix the original ref leaks and the refcount stays at 2.
     */
    public void testMergeResultWriteToDoesNotLeakPooledTopHitsOnPreBatchedPath() throws Exception {
        TransportVersion outVersion = TransportVersionUtils.randomVersionNotSupporting(BATCHED_QUERY_EXECUTION_DELAYABLE_WRITEABLE);

        InternalAggregations pooledAggs = roundTripToPooledTopHits(createUnpooledAggregationsWithTopHits(), TransportVersion.current());
        SearchHits pooledHits = pooledAggs.<InternalTopHits>get("th").getHits();
        assertTrue("round-trip must yield pooled SearchHits (hit with _source)", pooledHits.isPooled());

        List<SearchHits> transientHits = new ArrayList<>();
        Writeable.Reader<InternalAggregations> capturingReader = in -> {
            InternalAggregations aggs = InternalAggregations.readFrom(in);
            InternalAggregations.addTopHitsToReleaseList(aggs, transientHits, true);
            return aggs;
        };

        try (
            DelayableWriteable<InternalAggregations> serialized = DelayableWriteable.referencing(pooledAggs)
                .asSerialized(capturingReader, writableRegistry())
        ) {
            assertTrue(serialized.isSerialized());

            QueryPhaseResultConsumer.MergeResult mergeResult = new QueryPhaseResultConsumer.MergeResult(List.of(), null, serialized, 0L);

            BytesStreamOutput out = new BytesStreamOutput();
            out.setTransportVersion(outVersion);
            mergeResult.writeTo(out);

            assertFalse("pre-BATCHED path must have triggered expand() — no transient hits were captured", transientHits.isEmpty());

            // Without fix: decRef returns false (original ref leaked; our extra ref was not the last).
            // With fix: decRef returns true (original was released; our extra ref is the last → count hits 0).
            // Remove before decRef to prevent double-release in the finally block.
            for (var it = transientHits.iterator(); it.hasNext();) {
                SearchHits h = it.next();
                it.remove();
                assertTrue("transient SearchHits must be released after pre-BATCHED expand + write", h.decRef());
            }
        } finally {
            // Release any extra refs not yet consumed above (e.g. when writeTo threw before the assertion).
            for (SearchHits h : transientHits) {
                h.decRef();
            }
            pooledHits.decRef();
        }
    }

    private static InternalAggregations createUnpooledAggregationsWithTopHits() {
        TopDocsAndMaxScore topDocs = new TopDocsAndMaxScore(
            new TopDocs(new TotalHits(1, TotalHits.Relation.EQUAL_TO), new ScoreDoc[] { new ScoreDoc(0, 1.0f) }),
            Float.NaN
        );
        SearchHit hit = new SearchHit(0, "id");
        hit.sourceRef(Source.fromMap(Map.of("f", "v"), XContentType.JSON).internalSourceRef());
        hit.score(1.0f);
        // Pooled top_hits shape (not SearchHits.unpooled — that forbids pooled hits); wire round-trip asserts pooled reads.
        SearchHits hits = new SearchHits(new SearchHit[] { hit }, new TotalHits(1, TotalHits.Relation.EQUAL_TO), 1.0f);
        InternalTopHits topHits = new InternalTopHits("th", 0, 10, topDocs, hits, Map.of());
        return InternalAggregations.from(singletonList(topHits));
    }

    private InternalAggregations roundTripToPooledTopHits(InternalAggregations aggs, TransportVersion version) throws IOException {
        BytesStreamOutput output = new BytesStreamOutput();
        output.setTransportVersion(version);
        output.writeWriteable(aggs);
        try (StreamInput in = output.bytes().streamInput()) {
            in.setTransportVersion(version);
            NamedWriteableAwareStreamInput namedIn = new NamedWriteableAwareStreamInput(in, writableRegistry());
            return InternalAggregations.readFrom(namedIn);
        }
    }

}
