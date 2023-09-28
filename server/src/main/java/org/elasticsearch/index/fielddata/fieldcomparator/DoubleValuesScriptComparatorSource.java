/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.fielddata.fieldcomparator;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.LeafFieldComparator;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.comparators.DoubleComparator;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.index.fielddata.NumericDoubleValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.script.DocValuesDocReader;
import org.elasticsearch.script.NumberSortScript;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.search.sort.BucketedSort;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;

/**
 * Comparator source for double values.
 */
public class DoubleValuesScriptComparatorSource extends DoubleValuesComparatorSource {

    private final NumberSortScript.LeafFactory scriptFactory;

    // TODO ask about this, given the comment // searchLookup is unnecessary here, as it's just used for expressions
    private final SearchLookup searchLookup;

    public DoubleValuesScriptComparatorSource(
        Object missingValue,
        MultiValueMode sortMode,
        Nested nested,
        NumberSortScript.LeafFactory scriptFactory,
        SearchLookup searchLookup
    ) {
        super(null, missingValue, sortMode, nested);
        this.scriptFactory = scriptFactory;
        this.searchLookup = searchLookup;
    }

    private SortedNumericDoubleValues getValuesFromScript(NumberSortScript leafScript) throws IOException {
        final NumericDoubleValues values = new NumericDoubleValues() {
            @Override
            public boolean advanceExact(int doc) {
                leafScript.setDocument(doc);
                return true;
            }

            @Override
            public double doubleValue() {
                return leafScript.execute();
            }
        };
        return FieldData.singleton(values);
    }

    @Override
    public FieldComparator<?> newComparator(String fieldname, int numHits, boolean enableSkipping, boolean reversed) {
        assert indexFieldData == null || fieldname.equals(indexFieldData.getFieldName());

        final double dMissingValue = (Double) missingObject(missingValue, reversed);
        // NOTE: it's important to pass null as a missing value in the constructor so that
        // the comparator doesn't check docsWithField since we replace missing values in select()
        return new DoubleComparator(numHits, null, null, reversed, false) {
            @Override
            public LeafFieldComparator getLeafComparator(LeafReaderContext context) throws IOException {
                NumberSortScript leafScript = scriptFactory.newInstance(new DocValuesDocReader(searchLookup, context));
                return new DoubleLeafComparator(context) {
                    @Override
                    protected NumericDocValues getNumericDocValues(LeafReaderContext context, String field) throws IOException {
                        return getNumericDoubleDocValues(context, dMissingValue, () -> getValuesFromScript(leafScript))
                            .getRawDoubleValues();
                    }

                    @Override
                    public void setScorer(Scorable scorer) {
                        leafScript.setScorer(scorer);
                    }
                };
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
        return new BucketedSort.ForDoubles(bigArrays, sortOrder, format, bucketSize, extra) {
            private final double dMissingValue = (Double) missingObject(missingValue, sortOrder == SortOrder.DESC);

            @Override
            public Leaf forLeaf(LeafReaderContext leafReaderContext) throws IOException {
                NumberSortScript leafScript = scriptFactory.newInstance(new DocValuesDocReader(searchLookup, leafReaderContext));
                return new Leaf(leafReaderContext) {
                    private final NumericDoubleValues docValues = getNumericDoubleDocValues(
                        leafReaderContext,
                        dMissingValue,
                        () -> getValuesFromScript(leafScript)
                    );
                    private double docValue;

                    // TODO this is weird, why don't we just call the script? Seems like this is how it was done prior...
                    @Override
                    protected boolean advanceExact(int doc) throws IOException {
                        if (docValues.advanceExact(doc)) {
                            docValue = docValues.doubleValue();
                            return true;
                        }
                        return false;
                    }

                    @Override
                    protected double docValue() {
                        return docValue;
                    }
                };
            }
        };
    }
}
