/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.analytics.stringstats;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.IntArray;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.metrics.MetricsAggregator;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Metric aggregator that operates over string and returns statistics such as
 * minimum length, maximum length, average length, the total Shannon entropy and
 * probability distribution for each character appearing in all terms.
 */
public class StringStatsAggregator extends MetricsAggregator {

    final ValuesSource.Bytes valuesSource;
    final DocValueFormat format;

    /** Option to show the probability distribution for each character appearing in all terms. */
    private final boolean showDistribution;

    LongArray count;
    IntArray minLength;
    IntArray maxLength;
    /** Accummulates the total length of all fields. Used for calculate average length and char frequencies. */
    LongArray totalLength;
    /** Map that stores the number of occurrences for each character. */
    Map<Character, LongArray> charOccurrences;

    StringStatsAggregator(String name, ValuesSource valuesSource, boolean showDistribution, DocValueFormat format,
                          SearchContext context, Aggregator parent, Map<String, Object> metadata) throws IOException {
        super(name, context, parent, metadata);
        this.showDistribution = showDistribution;
        this.valuesSource = (ValuesSource.Bytes) valuesSource;
        if (valuesSource != null) {
            final BigArrays bigArrays = context.bigArrays();
            count = bigArrays.newLongArray(1, true);
            totalLength = bigArrays.newLongArray(1, true);
            minLength = bigArrays.newIntArray(1, false);
            minLength.fill(0, minLength.size(), Integer.MAX_VALUE);
            maxLength = bigArrays.newIntArray(1, false);
            maxLength.fill(0, maxLength.size(), Integer.MIN_VALUE);
            charOccurrences = new HashMap<>();
        }
        this.format = format;
    }

    @Override
    public ScoreMode scoreMode() {
        return (valuesSource != null && valuesSource.needsScores()) ? ScoreMode.COMPLETE : ScoreMode.COMPLETE_NO_SCORES;
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx,
                                                final LeafBucketCollector sub) throws IOException {
        if (valuesSource == null) {
            return LeafBucketCollector.NO_OP_COLLECTOR;
        }
        final BigArrays bigArrays = context.bigArrays();
        final SortedBinaryDocValues values = valuesSource.bytesValues(ctx);

        return new LeafBucketCollectorBase(sub, values) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                final long overSize = BigArrays.overSize(bucket + 1);
                if (bucket >= count.size()) {
                    final long from = count.size();
                    count = bigArrays.resize(count, overSize);
                    totalLength = bigArrays.resize(totalLength, overSize);
                    minLength = bigArrays.resize(minLength, overSize);
                    maxLength = bigArrays.resize(maxLength, overSize);
                    minLength.fill(from, overSize, Integer.MAX_VALUE);
                    maxLength.fill(from, overSize, Integer.MIN_VALUE);
                }

                if (values.advanceExact(doc)) {
                    final int valuesCount = values.docValueCount();
                    count.increment(bucket, valuesCount);

                    for (int i = 0; i < valuesCount; i++) {
                        BytesRef value = values.nextValue();
                        if (value.length > 0) {
                            String valueStr = value.utf8ToString();
                            int length = valueStr.length();
                            totalLength.increment(bucket, length);

                            // Update min/max length for string
                            int min = Math.min(minLength.get(bucket), length);
                            int max = Math.max(maxLength.get(bucket), length);
                            minLength.set(bucket, min);
                            maxLength.set(bucket, max);

                            // Parse string chars and count occurrences
                            for (Character c : valueStr.toCharArray()) {
                                LongArray occ = charOccurrences.get(c);
                                if (occ == null) {
                                    occ = bigArrays.newLongArray(overSize, true);
                                } else {
                                    if (bucket >= occ.size()) {
                                        occ = bigArrays.resize(occ, overSize);
                                    }
                                }
                                occ.increment(bucket, 1);
                                charOccurrences.put(c, occ);
                            }
                        }
                    }
                }
            }
        };
    }

    @Override
    public InternalAggregation buildAggregation(long bucket) {
        if (valuesSource == null || bucket >= count.size()) {
            return buildEmptyAggregation();
        }

        // Convert Map entries: Character -> String and LongArray -> Long
        // Include only characters that have at least one occurrence
        Map<String, Long> occurrences = new HashMap<>(charOccurrences.size());
        for (Map.Entry<Character, LongArray> e : charOccurrences.entrySet()) {
            if (e.getValue().size() > bucket) {
                long occ = e.getValue().get(bucket);
                if (occ > 0) {
                    occurrences.put(e.getKey().toString(), occ);
                }
            }
        }

        return new InternalStringStats(name, count.get(bucket), totalLength.get(bucket),
            minLength.get(bucket), maxLength.get(bucket), occurrences, showDistribution,
            format, metadata());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalStringStats(name,
            0, 0, Integer.MAX_VALUE, Integer.MIN_VALUE,
            Collections.emptyMap(), showDistribution, format, metadata());
    }

    @Override
    public void doClose() {
        Releasables.close(maxLength, minLength, count, totalLength);
        if (charOccurrences != null) {
            Releasables.close(charOccurrences.values());
        }
    }
}
