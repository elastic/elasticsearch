/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.internal.hppc.IntArrayList;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.fielddata.FormattedDocValues;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.flattened.FlattenedFieldSyntheticWriterHelper;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Class that downsamples a label field for downsampling by keeping the last value.
 * Important note: This class assumes that field values are collected and sorted by descending order by time
 */
class LastValueFieldDownsampler extends AbstractFieldDownsampler<FormattedDocValues> {

    private final MappedFieldType fieldType;
    Object lastValue = null;

    // Downsamplers are shared across leaf collectors, but doc-values iterators are leaf-local and forward-only.
    // Keep the active iterator and read its docID() when switching back to it instead of storing per-leaf positions.
    private DocIdSetIterator leafDocIdIterator;
    private int leafDocIdIteratorDoc = -1;
    private boolean leafIteratorExhausted;

    LastValueFieldDownsampler(String name, MappedFieldType fieldType, IndexFieldData<?> fieldData) {
        super(name, fieldData);
        this.fieldType = fieldType;
    }

    /**
     * Creates a producer that can be used for downsampling labels.
     */
    static LastValueFieldDownsampler create(
        String name,
        MappedFieldType fieldType,
        IndexFieldData<?> fieldData,
        DownsamplerCountPerValueType fieldCounts
    ) {
        assert AggregateMetricDoubleFieldDownsampler.supportsFieldType(fieldType) == false
            && ExponentialHistogramFieldDownsampler.supportsFieldType(fieldType) == false
            && TDigestHistogramFieldDownsampler.supportsFieldType(fieldType) == false
            : "field '" + name + "' of type '" + fieldType.typeName() + "' should be processed by a dedicated downsampler";
        fieldCounts.increaseFormattedValueFields();
        if ("flattened".equals(fieldType.typeName())) {
            return new LastValueFieldDownsampler.FlattenedFieldProducer(name, fieldType, fieldData);
        }
        return new LastValueFieldDownsampler(name, fieldType, fieldData);
    }

    @Override
    public void reset() {
        state = State.EMPTY;
        lastValue = null;
    }

    @Override
    public void collect(FormattedDocValues docValues, IntArrayList docIdBuffer) throws IOException {
        if (isDone() || docIdBuffer.isEmpty()) {
            return;
        }

        DocIdSetIterator docIdIterator = docValues.docIdIterator();
        if (docIdIterator == null) {
            collectUsingAdvanceExact(docValues, docIdBuffer);
            return;
        }

        resetLeafIteratorStateIfNeeded(docIdIterator);
        if (leafIteratorExhausted) {
            return;
        }

        collectUsingDocIdIterator(docValues, docIdBuffer);
    }

    private void collectUsingAdvanceExact(FormattedDocValues docValues, IntArrayList docIdBuffer) throws IOException {
        for (int i = 0; i < docIdBuffer.size() && isDone() == false; i++) {
            int docId = docIdBuffer.get(i);
            if (docValues.advanceExact(docId) == false) {
                continue;
            }
            collectCurrentValues(docValues);
        }
    }

    private void resetLeafIteratorStateIfNeeded(DocIdSetIterator docIdIterator) {
        if (leafDocIdIterator != docIdIterator) {
            // If we see a previously used iterator again, its own docID() is the last position for that leaf.
            leafDocIdIterator = docIdIterator;
            leafDocIdIteratorDoc = docIdIterator.docID();
            leafIteratorExhausted = leafDocIdIteratorDoc == DocIdSetIterator.NO_MORE_DOCS;
        }
    }

    /**
     * Collects buffered docs by intersecting the ordered buffer with the numeric doc-values iterator.
     * <p>
     * This path is used only when the field exposes a {@link DocIdSetIterator}. The iterator is leaf-local
     * and forward-only, so we never try to rewind it. When collection later returns to a previously seen
     * leaf, {@link #resetLeafIteratorStateIfNeeded(DocIdSetIterator)} reads the iterator's own
     * {@link DocIdSetIterator#docID()} and resumes from that position.
     *
     * <pre>
     * buffered doc ids:  [ 3 ][ 7 ][ 9 ][ 15 ]
     * doc-values docs:   [ 1 ][ 3 ][ 8 ][ 9 ][ 20 ]
     *
     * 1. Advance the doc-values iterator to the first buffered doc or beyond.
     *
     *    buffered doc ids:  [ 3 ][ 7 ][ 9 ][ 15 ]
     *                         ^
     *    doc-values docs:   [ 1 ][ 3 ][ 8 ][ 9 ][ 20 ]
     *                         ^
     *
     * 2. Keep comparing the current doc-values doc to the current buffered target:
     *
     *    current doc &lt; target doc  -> advance doc-values to target doc
     *    current doc == target doc -> collect current values, then move to the next buffered target
     *    current doc &gt; target doc  -> binary-search the next buffered target that is >= current doc
     *
     * If the iterator is exhausted, the leaf is marked exhausted and later collections for the same iterator
     * return without touching doc values. This turns the problem into a forward-only merge instead of probing
     * every buffered doc with {@code advanceExact(docId)}.
     * </pre>
     */
    private void collectUsingDocIdIterator(FormattedDocValues docValues, IntArrayList docIdBuffer) throws IOException {
        int bufferedDocCount = docIdBuffer.size();
        int[] bufferedDocIds = docIdBuffer.buffer;
        if (leafDocIdIteratorDoc > bufferedDocIds[bufferedDocCount - 1]) {
            return;
        }

        int firstBufferedDocId = bufferedDocIds[0];
        if (leafDocIdIteratorDoc < firstBufferedDocId) {
            // advance to the closest doc that >= firstBufferedDocId
            advanceLeafDocIdIterator(firstBufferedDocId);
            if (leafIteratorExhausted) {
                return;
            }
        }

        // find the closest index in bufferedDocIds that points to a doc that is >= leafDocIdIteratorDoc
        int index = lowerBound(bufferedDocIds, 0, bufferedDocCount, leafDocIdIteratorDoc);
        while (index < bufferedDocCount && leafIteratorExhausted == false && isDone() == false) {
            int targetDocId = bufferedDocIds[index];
            if (leafDocIdIteratorDoc < targetDocId) {
                // advance to the closest doc that is >= targetDocId
                advanceLeafDocIdIterator(targetDocId);
                continue;
            }

            // found the intersection (means that the particular field exists for the current doc)
            if (leafDocIdIteratorDoc == targetDocId) {
                collectCurrentValues(docValues);
                index++;
                if (index < bufferedDocCount && isDone() == false) {
                    advanceLeafDocIdIterator(bufferedDocIds[index]);
                }
                continue;
            }

            // move to the next buffered doc, which is the closest to the next doc of the particular field
            index = lowerBound(bufferedDocIds, index + 1, bufferedDocCount, leafDocIdIteratorDoc);
        }
    }

    private void advanceLeafDocIdIterator(int targetDocId) throws IOException {
        leafDocIdIteratorDoc = leafDocIdIterator.advance(targetDocId);
        leafIteratorExhausted = leafDocIdIteratorDoc == DocIdSetIterator.NO_MORE_DOCS;
    }

    @Override
    public FormattedDocValues getLeaf(LeafReaderContext context) {
        DocValueFormat format = fieldType.docValueFormat(null, null);
        return fieldData.load(context).getFormattedValues(format);
    }

    /**
     * Collects the last value observed in these field values. This implementation assumes that field values are collected
     * and sorted by descending order by time. In this case, it assumes that the last value of the time is the first value
     * collected.  By setting state to {@link org.elasticsearch.xpack.downsample.AbstractFieldDownsampler.State#BUCKET_COMPLETED},
     * we ensure that the next values will be skipped for this bucket.
     */
    @Override
    public void collectCurrentValues(FormattedDocValues docValues) throws IOException {
        int docValuesCount = docValues.docValueCount();
        assert docValuesCount > 0;
        if (docValuesCount == 1) {
            lastValue = docValues.nextValue();
        } else {
            var values = new Object[docValuesCount];
            for (int j = 0; j < docValuesCount; j++) {
                values[j] = docValues.nextValue();
            }
            lastValue = values;
        }
        state = State.BUCKET_COMPLETED;
    }

    @Override
    public void write(XContentBuilder builder) throws IOException {
        if (isEmpty() == false) {
            builder.field(name(), lastValue);
        }
    }

    public Object lastValue() {
        return lastValue;
    }

    static final class FlattenedFieldProducer extends LastValueFieldDownsampler {

        private FlattenedFieldProducer(String name, MappedFieldType fieldType, IndexFieldData<?> fieldData) {
            super(name, fieldType, fieldData);
        }

        @Override
        public void write(XContentBuilder builder) throws IOException {
            if (isEmpty() == false) {
                builder.startObject(name());

                var value = lastValue();
                List<BytesRef> list;
                if (value instanceof Object[] values) {
                    list = new ArrayList<>(values.length);
                    for (Object v : values) {
                        list.add(new BytesRef(v.toString()));
                    }
                } else {
                    list = List.of(new BytesRef(value.toString()));
                }

                var iterator = list.iterator();
                FlattenedFieldSyntheticWriterHelper helper = new FlattenedFieldSyntheticWriterHelper(
                    () -> iterator.hasNext() ? iterator.next() : null,
                    FlattenedFieldSyntheticWriterHelper.SortedOffsetValues.NONE,
                    List.of()
                );
                helper.writeNested(builder);
                builder.endObject();
            }
        }
    }
}
