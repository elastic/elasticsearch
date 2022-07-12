/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.fielddata;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.fielddata.fieldcomparator.BytesRefFieldComparatorSource;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.script.field.DocValuesScriptFieldFactory;
import org.elasticsearch.script.field.ToScriptFieldFactory;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.search.lookup.SourceLookup;
import org.elasticsearch.search.sort.BucketedSort;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;

public class KeywordValueFetcherIndexFieldData
    implements
        IndexFieldData<KeywordValueFetcherIndexFieldData.KeywordValueFetcherLeafFieldData> {

    public static class Builder implements IndexFieldData.Builder {
        private final String name;
        private final ValueFetcher valueFetcher;
        private final SourceLookup sourceLookup;
        private final ToScriptFieldFactory<SortedBinaryDocValues> toScriptFieldFactory;

        public Builder(
            String name,
            ValueFetcher valueFetcher,
            SourceLookup sourceLookup,
            ToScriptFieldFactory<SortedBinaryDocValues> toScriptFieldFactory
        ) {
            this.name = name;
            this.valueFetcher = valueFetcher;
            this.sourceLookup = sourceLookup;
            this.toScriptFieldFactory = toScriptFieldFactory;
        }

        @Override
        public KeywordValueFetcherIndexFieldData build(IndexFieldDataCache cache, CircuitBreakerService breakerService) {
            return new KeywordValueFetcherIndexFieldData(name, valueFetcher, sourceLookup, toScriptFieldFactory);
        }
    }

    private final String fieldName;
    private final ValueFetcher valueFetcher;
    private final SourceLookup sourceLookup;
    private final ToScriptFieldFactory<SortedBinaryDocValues> toScriptFieldFactory;

    protected KeywordValueFetcherIndexFieldData(
        String fieldName,
        ValueFetcher valueFetcher,
        SourceLookup sourceLookup,
        ToScriptFieldFactory<SortedBinaryDocValues> toScriptFieldFactory
    ) {
        this.fieldName = fieldName;
        this.valueFetcher = valueFetcher;
        this.sourceLookup = sourceLookup;
        this.toScriptFieldFactory = toScriptFieldFactory;
    }

    @Override
    public String getFieldName() {
        return fieldName;
    }

    @Override
    public ValuesSourceType getValuesSourceType() {
        return CoreValuesSourceType.KEYWORD;
    }

    @Override
    public KeywordValueFetcherLeafFieldData load(LeafReaderContext context) {
        try {
            return loadDirect(context);
        } catch (Exception e) {
            throw ExceptionsHelper.convertToElastic(e);
        }
    }

    @Override
    public KeywordValueFetcherLeafFieldData loadDirect(LeafReaderContext context) throws Exception {
        return new KeywordValueFetcherLeafFieldData(toScriptFieldFactory, context, valueFetcher, sourceLookup);

    }

    @Override
    public SortField sortField(Object missingValue, MultiValueMode sortMode, XFieldComparatorSource.Nested nested, boolean reverse) {
        final XFieldComparatorSource source = new BytesRefFieldComparatorSource(this, missingValue, sortMode, nested);
        return new SortField(getFieldName(), source, reverse);
    }

    @Override
    public BucketedSort newBucketedSort(
        BigArrays bigArrays,
        Object missingValue,
        MultiValueMode sortMode,
        XFieldComparatorSource.Nested nested,
        SortOrder sortOrder,
        DocValueFormat format,
        int bucketSize,
        BucketedSort.ExtraData extra
    ) {
        throw new IllegalArgumentException("only supported on numeric fields");
    }

    public static class KeywordValueFetcherLeafFieldData implements LeafFieldData {

        private final ToScriptFieldFactory<SortedBinaryDocValues> toScriptFieldFactory;
        private final LeafReaderContext leafReaderContext;

        private final ValueFetcher valueFetcher;
        private final SourceLookup sourceLookup;

        public KeywordValueFetcherLeafFieldData(
            ToScriptFieldFactory<SortedBinaryDocValues> toScriptFieldFactory,
            LeafReaderContext leafReaderContext,
            ValueFetcher valueFetcher,
            SourceLookup sourceLookup
        ) {
            this.toScriptFieldFactory = toScriptFieldFactory;
            this.leafReaderContext = leafReaderContext;
            this.valueFetcher = valueFetcher;
            this.sourceLookup = sourceLookup;
        }

        @Override
        public long ramBytesUsed() {
            return 0;
        }

        @Override
        public void close() {

        }

        @Override
        public DocValuesScriptFieldFactory getScriptFieldFactory(String name) {
            return toScriptFieldFactory.getScriptFieldFactory(getBytesValues(), name);
        }

        @Override
        public SortedBinaryDocValues getBytesValues() {
            return new KeywordValueFetcherBinaryDocValues(leafReaderContext, valueFetcher, sourceLookup);
        }
    }

    public static class KeywordValueFetcherBinaryDocValues extends SortedBinaryDocValues implements ValueFetcherDocValues {

        private final LeafReaderContext leafReaderContext;

        private final ValueFetcher valueFetcher;
        private final SourceLookup sourceLookup;

        private SortedSet<Object> values;
        private Iterator<Object> iterator;

        public KeywordValueFetcherBinaryDocValues(
            LeafReaderContext leafReaderContext,
            ValueFetcher valueFetcher,
            SourceLookup sourceLookup
        ) {
            this.leafReaderContext = leafReaderContext;
            this.valueFetcher = valueFetcher;
            this.sourceLookup = sourceLookup;
        }

        @Override
        public boolean advanceExact(int doc) throws IOException {
            sourceLookup.setSegmentAndDocument(leafReaderContext, doc);
            values = new TreeSet<>(valueFetcher.fetchValues(sourceLookup, Collections.emptyList()));
            iterator = values.iterator();

            return true;
        }

        @Override
        public int docValueCount() {
            return values.size();
        }

        @Override
        public BytesRef nextValue() throws IOException {
            assert iterator.hasNext();
            return new BytesRef(iterator.next().toString());
        }
    }

    /**
     * Marker interface to indicate these doc values are generated
     * on-the-fly from a {@code ValueFetcher}.
     */
    public interface ValueFetcherDocValues {
        // marker interface
    }
}
