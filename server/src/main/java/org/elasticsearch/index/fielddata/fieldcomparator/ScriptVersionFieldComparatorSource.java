/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.fielddata.fieldcomparator;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.Pruning;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.index.fielddata.AbstractBinaryDocValues;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.script.BytesRefProducer;
import org.elasticsearch.script.BytesRefSortScript;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.sort.BucketedSort;
import org.elasticsearch.search.sort.ScriptSortBuilder;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;

/**
 * Script comparator source for version values.
 */
public class ScriptVersionFieldComparatorSource extends BytesRefFieldComparatorSource {

    private final CheckedFunction<LeafReaderContext, BytesRefSortScript, IOException> scriptSupplier;
    private final DocValueFormat scriptResultValueFormat;

    public ScriptVersionFieldComparatorSource(
        CheckedFunction<LeafReaderContext, BytesRefSortScript, IOException> scriptSupplier,
        DocValueFormat scriptResultValueFormat,
        IndexFieldData<?> indexFieldData,
        Object missingValue,
        MultiValueMode sortMode,
        Nested nested
    ) {
        super(indexFieldData, missingValue, sortMode, nested);
        this.scriptResultValueFormat = scriptResultValueFormat;
        this.scriptSupplier = scriptSupplier;
    }

    private SortedBinaryDocValues getValues(BytesRefSortScript leafScript) throws IOException {
        final BinaryDocValues values = new AbstractBinaryDocValues() {

            @Override
            public boolean advanceExact(int doc) {
                leafScript.setDocument(doc);
                return true;
            }

            @Override
            public BytesRef binaryValue() {
                Object result = leafScript.execute();
                if (result == null) {
                    return null;
                }
                if (result instanceof BytesRefProducer) {
                    return ((BytesRefProducer) result).toBytesRef();
                }

                if (scriptResultValueFormat == null) {
                    throw new IllegalArgumentException("Invalid sort type: version");
                }
                return scriptResultValueFormat.parseBytesRef(result);
            }
        };
        return FieldData.singleton(values);
    }

    @Override
    protected FieldComparator<?> newComparatorWithoutOrdinal(
        String fieldname,
        int numHits,
        Pruning enableSkipping,
        boolean reversed,
        BytesRef missingBytes,
        boolean sortMissingLast
    ) {
        return new FieldComparator.TermValComparator(numHits, null, sortMissingLast) {
            BytesRefSortScript leafScript;

            @Override
            protected BinaryDocValues getBinaryDocValues(LeafReaderContext context, String field) throws IOException {
                leafScript = scriptSupplier.apply(context);
                return ScriptVersionFieldComparatorSource.this.getBinaryDocValues(context, missingBytes, getValues(leafScript));
            }

            @Override
            public void setScorer(Scorable scorer) {
                leafScript.setScorer(scorer);
            }
        };
    }

    @Override
    public BucketedSort newBucketedSort(
        BigArrays bigArrays,
        SortOrder sortOrder,
        DocValueFormat format,
        int bucketSize,
        BucketedSort.ExtraData extra
    ) {
        throw new IllegalArgumentException(
            "error building sort for [_script]: "
                + "script sorting only supported on [numeric] scripts but was ["
                + ScriptSortBuilder.ScriptSortType.VERSION
                + "]"
        );
    }
}
