/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.bucket.composite;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.PriorityQueue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.Strings;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.StringFieldType;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.LeafBucketCollector;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayDeque;
import java.util.List;
import java.util.function.BiConsumer;

/**
 * A {@link SingleDimensionValuesSource} for global ordinals.
 */
class GlobalOrdinalValuesSource extends SingleDimensionValuesSource<BytesRef> {

    private static final Logger logger = LogManager.getLogger(GlobalOrdinalValuesSource.class);

    public static final int MAX_TERMS_FOR_DYNAMIC_PRUNING = 128;

    public static final long MISSING_VALUE_FLAG = -1L;
    private final long uniqueValueCount;
    private final CheckedFunction<LeafReaderContext, SortedSetDocValues, IOException> docValuesFunc;
    private LongArray values;
    private SortedSetDocValues lookup;
    private long currentValue;
    private Long afterValueGlobalOrd;
    private Long highestCompetitiveValueGlobalOrd;
    private boolean isTopValueInsertionPoint;
    private CompetitiveIterator currentCompetitiveIterator;
    private int segmentsDynamicPruningUsed;
    private int totalSegments;

    private long lastLookupOrd = -1;
    private BytesRef lastLookupValue;

    GlobalOrdinalValuesSource(
        BigArrays bigArrays,
        MappedFieldType type,
        long uniqueValueCount,
        CheckedFunction<LeafReaderContext, SortedSetDocValues, IOException> docValuesFunc,
        DocValueFormat format,
        boolean missingBucket,
        MissingOrder missingOrder,
        int size,
        int reverseMul
    ) {
        super(bigArrays, format, type, missingBucket, missingOrder, reverseMul);
        this.uniqueValueCount = uniqueValueCount;
        this.docValuesFunc = docValuesFunc;
        this.values = bigArrays.newLongArray(Math.min(size, 100), false);
    }

    /**
     * Return the number of unique values of this source. Note that some unique
     * values might not produce buckets if the query doesn't match documents
     * that contain these values.
     */
    long getUniqueValueCount() {
        return uniqueValueCount;
    }

    /**
     * Return whether the source can be used for dynamic pruning.
     */
    boolean mayDynamicallyPrune(IndexReader indexReader) {
        // If missing bucket is requested we have no way to tell lucene to efficiently match
        // buckets in a range AND missing values.
        if (missingBucket) {
            return false;
        }

        // If we do not know the field type, the field is not indexed (e.g. runtime field) or the field
        // has no doc values we should not apply the optimization.
        if (fieldType == null || fieldType.isIndexed() == false || fieldType.hasDocValues() == false) {
            return false;
        }

        // We also need to check if there are terms for the field in question.
        // Some field types do not despite being global ordinals (e.g. IP addresses).
        return hasTerms(indexReader);
    }

    private boolean hasTerms(IndexReader indexReader) {
        assert fieldType != null;
        List<LeafReaderContext> leaves = indexReader.leaves();
        for (LeafReaderContext leaf : leaves) {
            Terms terms;
            try {
                terms = leaf.reader().terms(fieldType.name());
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            if (terms != null) {
                return true;
            }
        }
        return false;
    }

    @Override
    void copyCurrent(int slot) {
        values = bigArrays.grow(values, slot + 1);
        values.set(slot, currentValue);
    }

    private int compareInternal(long lhs, long rhs) {
        int mul = (lhs == MISSING_VALUE_FLAG || rhs == MISSING_VALUE_FLAG) ? missingOrder.compareAnyValueToMissing(reverseMul) : reverseMul;
        return Long.compare(lhs, rhs) * mul;
    }

    @Override
    int compare(int from, int to) {
        return compareInternal(values.get(from), values.get(to));
    }

    @Override
    int compareCurrent(int slot) {
        return compareInternal(currentValue, values.get(slot));
    }

    @Override
    int compareCurrentWithAfter() {
        int cmp = compareInternal(currentValue, afterValueGlobalOrd);
        if (cmp == 0 && isTopValueInsertionPoint) {
            // the top value is missing in this shard, the comparison is against
            // the insertion point of the top value so equality means that the value
            // is "after" the insertion point.
            return missingOrder.compareAnyValueToMissing(reverseMul);
        }
        return cmp;
    }

    @Override
    int hashCode(int slot) {
        return Long.hashCode(values.get(slot));
    }

    @Override
    int hashCodeCurrent() {
        return Long.hashCode(currentValue);
    }

    @Override
    void setAfter(Comparable<?> value) {
        if (missingBucket && value == null) {
            afterValue = null;
            afterValueGlobalOrd = MISSING_VALUE_FLAG;
        } else if (value.getClass() == String.class || (fieldType == null)) {
            // the value might be not string if this field is missing in this shard but present in other shards
            // and doesn't have a string type
            afterValue = format.parseBytesRef(value);
        } else if (value.getClass() == BytesRef.class) {
            // The value may be a bytes reference (eg an encoded tsid field)
            afterValue = (BytesRef) value;
        } else {
            throw new IllegalArgumentException("invalid value, expected string, got " + value.getClass().getSimpleName());
        }
    }

    @Override
    BytesRef toComparable(int slot) throws IOException {
        long globalOrd = values.get(slot);
        if (missingBucket && globalOrd == MISSING_VALUE_FLAG) {
            return null;
        } else if (globalOrd == lastLookupOrd) {
            return lastLookupValue;
        } else {
            lastLookupOrd = globalOrd;
            lastLookupValue = BytesRef.deepCopyOf(lookup.lookupOrd(values.get(slot)));
            return lastLookupValue;
        }
    }

    @Override
    LeafBucketCollector getLeafCollector(LeafReaderContext context, LeafBucketCollector next) throws IOException {
        totalSegments++;
        final SortedSetDocValues dvs = docValuesFunc.apply(context);
        if (lookup == null) {
            initLookup(dvs);
        }

        // We create a competitive iterator that allows us to optimize by narrowing down the search
        // to terms that are within the range based on what the composite queue is tracking.
        // For example, if the composite agg size is 5, and we have seen terms with ordinals [1, 4, 5, 10, 11],
        // we know that we never need to look at terms with and ordinal higher than 11.
        final CompetitiveIterator competitiveIterator = fieldType == null ? null : new CompetitiveIterator(context, fieldType.name());
        currentCompetitiveIterator = competitiveIterator;

        final SortedDocValues singleton = DocValues.unwrapSingleton(dvs);
        if (singleton != null) {
            return new LeafBucketCollector() {

                @Override
                public void collect(int doc, long bucket) throws IOException {
                    if (singleton.advanceExact(doc)) {
                        currentValue = singleton.ordValue();
                        next.collect(doc, bucket);
                    } else if (missingBucket) {
                        currentValue = MISSING_VALUE_FLAG;
                        next.collect(doc, bucket);
                    }
                }

                @Override
                public DocIdSetIterator competitiveIterator() {
                    return competitiveIterator;
                }
            };
        } else {
            return new LeafBucketCollector() {

                @Override
                public void collect(int doc, long bucket) throws IOException {
                    if (dvs.advanceExact(doc)) {
                        for (int i = 0; i < dvs.docValueCount(); i++) {
                            currentValue = dvs.nextOrd();
                            next.collect(doc, bucket);
                        }
                    } else if (missingBucket) {
                        currentValue = MISSING_VALUE_FLAG;
                        next.collect(doc, bucket);
                    }
                }

                @Override
                public DocIdSetIterator competitiveIterator() {
                    return competitiveIterator;
                }
            };
        }
    }

    @Override
    LeafBucketCollector getLeafCollector(Comparable<BytesRef> value, LeafReaderContext context, LeafBucketCollector next)
        throws IOException {
        if (value.getClass() != BytesRef.class) {
            throw new IllegalArgumentException("Expected BytesRef, got " + value.getClass());
        }
        totalSegments++;
        BytesRef term = (BytesRef) value;
        final SortedSetDocValues dvs = docValuesFunc.apply(context);
        if (lookup == null) {
            initLookup(dvs);
        }
        final SortedDocValues singleton = DocValues.unwrapSingleton(dvs);
        if (singleton != null) {
            return new LeafBucketCollector() {
                boolean currentValueIsSet = false;

                @Override
                public void collect(int doc, long bucket) throws IOException {
                    if (currentValueIsSet == false) {
                        if (singleton.advanceExact(doc)) {
                            long ord = singleton.ordValue();
                            if (term.equals(lookup.lookupOrd(ord))) {
                                currentValueIsSet = true;
                                currentValue = ord;
                            }
                        }
                    }
                    assert currentValueIsSet;
                    next.collect(doc, bucket);
                }
            };
        } else {
            return new LeafBucketCollector() {
                boolean currentValueIsSet = false;

                @Override
                public void collect(int doc, long bucket) throws IOException {
                    if (currentValueIsSet == false) {
                        if (dvs.advanceExact(doc)) {
                            for (int i = 0; i < dvs.docValueCount(); i++) {
                                long ord = dvs.nextOrd();
                                if (term.equals(lookup.lookupOrd(ord))) {
                                    currentValueIsSet = true;
                                    currentValue = ord;
                                    break;
                                }
                            }
                        }
                    }
                    assert currentValueIsSet;
                    next.collect(doc, bucket);
                }
            };
        }
    }

    @Override
    SortedDocsProducer createSortedDocsProducerOrNull(IndexReader reader, Query query) {
        if (checkIfSortedDocsIsApplicable(reader, fieldType) == false
            || fieldType instanceof StringFieldType == false
            || (query != null && query.getClass() != MatchAllDocsQuery.class)) {
            return null;
        }
        return new TermsSortedDocsProducer(fieldType.name());
    }

    @Override
    public void close() {
        Releasables.close(values);
    }

    private void initLookup(SortedSetDocValues dvs) throws IOException {
        lookup = dvs;
        if (afterValue != null && afterValueGlobalOrd == null) {
            afterValueGlobalOrd = lookup.lookupTerm(afterValue);
            if (afterValueGlobalOrd < 0) {
                // convert negative insert position
                afterValueGlobalOrd = -afterValueGlobalOrd - 1;
                isTopValueInsertionPoint = true;
            }
        }
    }

    public void updateHighestCompetitiveValue(int slot) throws IOException {
        highestCompetitiveValueGlobalOrd = values.get(slot);
        logger.trace("Highest observed set to [{}]", highestCompetitiveValueGlobalOrd);
        final CompetitiveIterator competitiveIterator = currentCompetitiveIterator;
        if (competitiveIterator != null) {
            competitiveIterator.updateBounds();
        }
    }

    void collectDebugInfo(String namespace, BiConsumer<String, Object> add) {
        add.accept(Strings.format("%s.segments_collected", namespace), totalSegments);
        add.accept(Strings.format("%s.segments_dynamic_pruning_used", namespace), segmentsDynamicPruningUsed);
    }

    private record PostingsEnumAndOrd(PostingsEnum postings, long ord) {}

    private class CompetitiveIterator extends DocIdSetIterator {

        private final LeafReaderContext context;
        private final int maxDoc;
        private final String field;
        private int doc = -1;
        private ArrayDeque<PostingsEnumAndOrd> postings;
        private DocIdSetIterator docsWithField;
        private PriorityQueue<PostingsEnumAndOrd> disjunction;

        CompetitiveIterator(LeafReaderContext context, String field) {
            this.context = context;
            this.maxDoc = context.reader().maxDoc();
            this.field = field;
        }

        @Override
        public int docID() {
            return doc;
        }

        @Override
        public int nextDoc() throws IOException {
            return advance(docID() + 1);
        }

        @Override
        public int advance(int target) throws IOException {
            if (target >= maxDoc) {
                return doc = NO_MORE_DOCS;
            } else if (disjunction == null) {
                if (docsWithField != null) {
                    return doc = docsWithField.advance(target);
                } else {
                    // We haven't started skipping yet
                    return doc = target;
                }
            } else {
                PostingsEnumAndOrd top = disjunction.top();
                if (top == null) {
                    // priority queue is empty, none of the remaining documents are competitive
                    return doc = NO_MORE_DOCS;
                }
                while (top.postings.docID() < target) {
                    top.postings.advance(target);
                    top = disjunction.updateTop();
                }
                return doc = top.postings.docID();
            }
        }

        @Override
        public long cost() {
            return context.reader().maxDoc();
        }

        /**
         * Update this iterator to only match postings whose term has an ordinal between {@code minOrd}
         * included and {@code maxOrd} included.
         */
        private void update(long minOrd, long maxOrd) throws IOException {
            final int maxTerms = Math.min(MAX_TERMS_FOR_DYNAMIC_PRUNING, IndexSearcher.getMaxClauseCount());
            final long size = Math.max(0, maxOrd - minOrd + 1);
            if (size > maxTerms) {
                if (docsWithField == null) {
                    docsWithField = docValuesFunc.apply(context);
                }
            } else if (postings == null) {
                init(minOrd, maxOrd);
            } else {
                boolean removed = false;
                // Zero or more ords got removed
                while (postings.isEmpty() == false && postings.getFirst().ord < minOrd) {
                    removed = true;
                    postings.removeFirst();
                }
                while (postings.isEmpty() == false && postings.getLast().ord > maxOrd) {
                    removed = true;
                    postings.removeLast();
                }
                if (removed) {
                    disjunction.clear();
                    disjunction.addAll(postings);
                }
            }
        }

        /**
         * For the first time, this iterator is allowed to skip documents. It needs to pull {@link
         * PostingsEnum}s from the terms dictionary of the inverted index and create a priority queue
         * out of them.
         */
        private void init(long minOrd, long maxOrd) throws IOException {
            segmentsDynamicPruningUsed++;

            final int size = (int) Math.max(0, maxOrd - minOrd + 1);
            postings = new ArrayDeque<>(size);
            if (size > 0) {
                Terms terms = context.reader().terms(field);
                if (terms != null) {
                    BytesRef minTerm = BytesRef.deepCopyOf(lookup.lookupOrd(minOrd));
                    TermsEnum termsEnum = terms.iterator();
                    TermsEnum.SeekStatus seekStatus = termsEnum.seekCeil(minTerm);
                    if (seekStatus != TermsEnum.SeekStatus.END) {
                        BytesRef maxTerm = BytesRef.deepCopyOf(lookup.lookupOrd(maxOrd));

                        TermsEnum globalTermsEnum = lookup.termsEnum();
                        globalTermsEnum.seekExact(minOrd);

                        for (BytesRef term = termsEnum.term(); term != null && term.compareTo(maxTerm) <= 0; term = termsEnum.next()) {
                            // Compute the global ordinal of this term by advancing the global terms enum to the same term and retrieving
                            // the term ordinal. This is cheaper than calling lookupTerm for every term.
                            while (globalTermsEnum.term().compareTo(term) < 0) {
                                BytesRef nextGlobalTerm = globalTermsEnum.next();
                                assert nextGlobalTerm != null;
                            }
                            assert globalTermsEnum.term().equals(term);
                            final long globalOrd = globalTermsEnum.ord();
                            postings.add(new PostingsEnumAndOrd(termsEnum.postings(null, PostingsEnum.NONE), globalOrd));
                        }
                    }
                }
            }
            disjunction = new PriorityQueue<>(size) {
                @Override
                protected boolean lessThan(PostingsEnumAndOrd a, PostingsEnumAndOrd b) {
                    return a.postings.docID() < b.postings.docID();
                }
            };
            disjunction.addAll(postings);
        }

        public void updateBounds() throws IOException {
            if (highestCompetitiveValueGlobalOrd == null) {
                return;
            }

            // TODO If this is the only source, we know we are done with the buckets of the after_key.
            // We could optimize even further by skipping to the next global ordinal after the after_key.

            long lowOrd;
            if (afterValueGlobalOrd != null && afterValueGlobalOrd != MISSING_VALUE_FLAG) {
                lowOrd = afterValueGlobalOrd;
            } else {
                lowOrd = reverseMul == 1 ? 0 : lookup.getValueCount() - 1;
            }
            update(Math.min(highestCompetitiveValueGlobalOrd, lowOrd), Math.max(highestCompetitiveValueGlobalOrd, lowOrd));
        }
    }
}
