/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.bucket.terms;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.columnar.string.StringColumnSource;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.fielddata.ColumnarPayloadSortableBinaryDocValues;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.bucket.terms.MapStringTermsAggregator.BucketCollectConsumer;

import java.io.IOException;
import java.util.Arrays;
import java.util.function.LongConsumer;

/**
 * Collects a {@code terms} aggregation over a ColumNAR keyword column with a dictionary. Hashing a term is the costly
 * part of the per-document loop, and a column has far fewer distinct terms than documents. This collector asks the
 * column for each document's term ordinal, which costs no decoding, and keeps the bucket of every ordinal it has seen
 * in the segment. A term is decoded and hashed once for each segment, and every other document goes straight to its
 * bucket.
 *
 * <p>Only the lookup from term bytes to bucket changes. Sub-aggregations, {@code _doc_count} and
 * {@code significant_terms} counting work as they do in the per-document loop, because the consumer still does the
 * counting. The collector knows nothing of how the column encodes its ordinals: {@link StringColumnSource#denseTermOrdinal}
 * says for each document whether it has no value, several, an escaped one, or which term its one value is.
 *
 * <p>A document is decided on its own. A document with one value, in the dictionary, is counted from the cache. An
 * escaped value is hashed for each document, as it is without the cache. A document with several values goes to the
 * per-document collector, which feeds the same buckets, so a segment holding some does not lose the cache for the rest.
 *
 * <p>The cache holds buckets of one owning bucket ordinal, zero. A document collected for another owning bucket goes to
 * the per-document collector.
 */
final class ColumnarOrdinalTermsCollector extends LeafBucketCollectorBase {

    /**
     * The largest dictionary the cache covers. The cache is cleared for each segment, so the cost of a segment grows
     * with the dictionary and not with the documents that match. Past this size, the cost of clearing it can be more
     * than the hashing it saves.
     *
     * <p>This number is an estimate, not a measurement. It was chosen as a round, conservative size. The benchmark covers
     * dictionaries of up to 5,000 terms only. At this size the cache is 2 MB, and a segment with few matched documents
     * may spend more clearing it than it saves.
     */
    static final int MAX_DICTIONARY_SIZE = 1 << 18;

    private static final long UNSEEN = -1;
    private static final long EXCLUDED = -2;

    /**
     * The bucket of each ordinal for the segment being collected. One instance serves all the segments of an
     * aggregator, so the memory it takes does not add up over the segments.
     */
    static final class OrdinalBuckets {
        private long[] buckets = new long[0];

        /** Clears the cache for a segment with a dictionary of {@code size} terms, charging the breaker for growth. */
        long[] reset(int size, LongConsumer addRequestCircuitBreakerBytes) {
            if (buckets.length < size) {
                addRequestCircuitBreakerBytes.accept((long) (size - buckets.length) * Long.BYTES);
                buckets = new long[size];
            }
            Arrays.fill(buckets, 0, size, UNSEEN);
            return buckets;
        }
    }

    /**
     * A collector that reads ordinals, or null when the values are not a column with a dictionary of a size the cache
     * covers, so that the caller keeps the per-document collector.
     *
     * @param fallback collects the documents this collector does not
     */
    @Nullable
    static LeafBucketCollector create(
        ColumnarPayloadSortableBinaryDocValues values,
        @Nullable IncludeExclude.StringFilter includeExclude,
        LeafBucketCollector sub,
        BucketCollectConsumer consumer,
        OrdinalBuckets ordinalBuckets,
        LongConsumer addRequestCircuitBreakerBytes,
        LeafBucketCollector fallback
    ) throws IOException {
        final StringColumnSource source = values.columnSource();
        if (source == null || source instanceof BinaryDocValues == false) {
            return null;
        }
        final int dictionarySize = source.reader().dictionarySize();
        if (source.reader().hasDictionary() && dictionarySize <= MAX_DICTIONARY_SIZE) {
            final long[] buckets = ordinalBuckets.reset(dictionarySize, addRequestCircuitBreakerBytes);
            return new ColumnarOrdinalTermsCollector(
                values,
                (BinaryDocValues) source,
                source,
                buckets,
                includeExclude,
                sub,
                consumer,
                fallback
            );
        }
        return null;
    }

    private final BinaryDocValues docValues;
    private final StringColumnSource source;
    private final long[] buckets;
    private final IncludeExclude.StringFilter includeExclude;
    private final LeafBucketCollector sub;
    private final BucketCollectConsumer consumer;
    private final LeafBucketCollector fallback;
    private final BytesRef scratch = new BytesRef();

    private ColumnarOrdinalTermsCollector(
        ColumnarPayloadSortableBinaryDocValues values,
        BinaryDocValues docValues,
        StringColumnSource source,
        long[] buckets,
        @Nullable IncludeExclude.StringFilter includeExclude,
        LeafBucketCollector sub,
        BucketCollectConsumer consumer,
        LeafBucketCollector fallback
    ) {
        super(sub, values);
        this.docValues = docValues;
        this.source = source;
        this.buckets = buckets;
        this.includeExclude = includeExclude;
        this.sub = sub;
        this.consumer = consumer;
        this.fallback = fallback;
    }

    @Override
    public void collect(int doc, long owningBucketOrd) throws IOException {
        if (owningBucketOrd != 0) {
            fallback.collect(doc, owningBucketOrd);
            return;
        }
        if (docValues.advanceExact(doc) == false) {
            return;
        }
        final int ordinal = source.denseTermOrdinal();
        if (ordinal == StringColumnSource.NO_VALUE) {
            return;
        }
        if (ordinal == StringColumnSource.MULTI_VALUED) {
            fallback.collect(doc, 0);
            return;
        }
        if (ordinal == StringColumnSource.ESCAPED) {
            final BytesRef escaped = source.escapedValue(scratch);
            if (includeExclude == null || includeExclude.accept(escaped)) {
                consumer.accept(sub, doc, 0, escaped);
            }
            return;
        }
        final long bucket = buckets[ordinal];
        if (bucket >= 0) {
            consumer.acceptExisting(sub, doc, bucket);
        } else if (bucket == UNSEEN) {
            final BytesRef term = source.termAt(ordinal, scratch);
            if (includeExclude == null || includeExclude.accept(term)) {
                buckets[ordinal] = consumer.accept(sub, doc, 0, term);
            } else {
                buckets[ordinal] = EXCLUDED;
            }
        } else {
            assert bucket == EXCLUDED : bucket;
        }
    }
}
