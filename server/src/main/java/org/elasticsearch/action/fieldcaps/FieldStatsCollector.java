/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.fieldcaps;

import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.tasks.CancellableTask;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * Reads {@link IndexFieldStats} for one shard from per-segment index metadata, so the cost stays independent of
 * the number of documents: keyword fields use term statistics plus a bounded sample of term lengths, numeric and
 * date fields use point statistics. Other field types are left out.
 */
final class FieldStatsCollector {
    /**
     * Terms sampled per segment to estimate the average length of a keyword value.
     */
    static final int TERM_LENGTH_SAMPLE = 64;

    private FieldStatsCollector() {}

    static Map<String, IndexFieldStats> collect(
        IndexReader reader,
        SearchExecutionContext context,
        Collection<String> fields,
        CancellableTask task
    ) throws IOException {
        Map<String, IndexFieldStats> stats = new HashMap<>();
        for (String field : fields) {
            task.ensureNotCancelled();
            if (context.isFieldMapped(field) == false) {
                continue;
            }
            IndexFieldStats fieldStats = collect(reader, field, context.getFieldType(field));
            if (fieldStats != null) {
                stats.put(field, fieldStats);
            }
        }
        return stats;
    }

    @Nullable
    private static IndexFieldStats collect(IndexReader reader, String field, MappedFieldType fieldType) throws IOException {
        if (fieldType instanceof KeywordFieldMapper.KeywordFieldType && fieldType.indexType().hasTerms()) {
            return terms(reader, field);
        }
        if (fieldType instanceof NumberFieldMapper.NumberFieldType numberType && fieldType.indexType().hasPoints()) {
            return points(reader, field, numberType::parsePoint);
        }
        if (fieldType instanceof DateFieldMapper.DateFieldType && fieldType.indexType().hasPoints()) {
            return points(reader, field, packed -> LongPoint.decodeDimension(packed, 0));
        }
        return null;
    }

    @Nullable
    private static IndexFieldStats terms(IndexReader reader, String field) throws IOException {
        long docsWithValue = 0;
        long valueCount = 0;
        double valueBytes = 0;
        for (LeafReaderContext leaf : reader.leaves()) {
            Terms terms = leaf.reader().terms(field);
            if (terms == null) {
                continue;
            }
            int docCount = terms.getDocCount();
            long sumDocFreq = terms.getSumDocFreq();
            if (docCount < 0 || sumDocFreq < 0) {
                return null;
            }
            docsWithValue += docCount;
            valueCount += sumDocFreq;
            double averageLength = sampleTermLength(leaf.reader(), field, terms);
            if (averageLength < 0) {
                return null;
            }
            valueBytes += averageLength * sumDocFreq;
        }
        return new IndexFieldStats(docsWithValue, valueCount, Math.round(valueBytes), null, null);
    }

    /**
     * Average byte length of a sample of the segment's distinct terms. Ordinals spread evenly over the doc values
     * dictionary give a sample that is not biased toward the lexicographically smallest terms; without doc values
     * the first terms of the terms dictionary are used.
     */
    private static double sampleTermLength(LeafReader reader, String field, Terms terms) throws IOException {
        long totalLength = 0;
        int sampled = 0;
        SortedSetDocValues docValues = reader.getSortedSetDocValues(field);
        if (docValues != null && docValues.getValueCount() > 0) {
            long ordinals = docValues.getValueCount();
            int samples = (int) Math.min(TERM_LENGTH_SAMPLE, ordinals);
            for (int i = 0; i < samples; i++) {
                totalLength += docValues.lookupOrd(i * ordinals / samples).length;
                sampled++;
            }
        } else {
            TermsEnum termsEnum = terms.iterator();
            BytesRef term;
            while (sampled < TERM_LENGTH_SAMPLE && (term = termsEnum.next()) != null) {
                totalLength += term.length;
                sampled++;
            }
        }
        return sampled == 0 ? -1 : (double) totalLength / sampled;
    }

    @Nullable
    private static IndexFieldStats points(IndexReader reader, String field, Function<byte[], Number> decode) throws IOException {
        long docsWithValue = 0;
        long valueCount = 0;
        Number min = null;
        Number max = null;
        for (LeafReaderContext leaf : reader.leaves()) {
            PointValues points = leaf.reader().getPointValues(field);
            if (points == null) {
                continue;
            }
            if (points.getNumDimensions() != 1) {
                return null;
            }
            docsWithValue += points.getDocCount();
            valueCount += points.size();
            Number segmentMin = normalize(decode.apply(points.getMinPackedValue()));
            Number segmentMax = normalize(decode.apply(points.getMaxPackedValue()));
            min = min == null || compare(segmentMin, min) < 0 ? segmentMin : min;
            max = max == null || compare(segmentMax, max) > 0 ? segmentMax : max;
        }
        return new IndexFieldStats(docsWithValue, valueCount, -1, min, max);
    }

    private static Number normalize(Number value) {
        if (value instanceof Double || value instanceof Float) {
            return value.doubleValue();
        }
        return value.longValue();
    }

    private static int compare(Number left, Number right) {
        if (left instanceof Long l && right instanceof Long r) {
            return Long.compare(l, r);
        }
        return Double.compare(left.doubleValue(), right.doubleValue());
    }
}
