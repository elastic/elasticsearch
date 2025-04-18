/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.fielddata.fieldcomparator;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.LeafFieldComparator;
import org.apache.lucene.search.Pruning;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.comparators.DoubleComparator;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.NumericDoubleValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.script.NumberSortScript;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.sort.BucketedSort;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;

/**
 * Script comparator source for double values.
 */
public class ScriptDoubleValuesComparatorSource extends DoubleValuesComparatorSource {

    private final CheckedFunction<LeafReaderContext, NumberSortScript, IOException> scriptSupplier;

    public ScriptDoubleValuesComparatorSource(
        CheckedFunction<LeafReaderContext, NumberSortScript, IOException> scriptSupplier,
        IndexNumericFieldData indexFieldData,
        @Nullable Object missingValue,
        MultiValueMode sortMode,
        Nested nested
    ) {
        super(indexFieldData, missingValue, sortMode, nested);
        this.scriptSupplier = scriptSupplier;
    }

    private SortedNumericDoubleValues getValues(NumberSortScript leafScript) throws IOException {
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

    private NumericDoubleValues getNumericDocValues(LeafReaderContext context, double missingValue, NumberSortScript leafScript)
        throws IOException {
        return getNumericDocValues(context, missingValue, getValues(leafScript));
    }

    @Override
    protected FieldComparator<?> newComparator(int numHits, Pruning enableSkipping, boolean reversed, double dMissingValue) {
        // NOTE: it's important to pass null as a missing value in the constructor so that
        // the comparator doesn't check docsWithField since we replace missing values in select()
        return new DoubleComparator(numHits, null, null, reversed, Pruning.NONE) {
            @Override
            public LeafFieldComparator getLeafComparator(LeafReaderContext context) throws IOException {
                NumberSortScript leafScript = scriptSupplier.apply(context);
                return new DoubleLeafComparator(context) {
                    @Override
                    protected NumericDocValues getNumericDocValues(LeafReaderContext context, String field) throws IOException {
                        return ScriptDoubleValuesComparatorSource.this.getNumericDocValues(context, dMissingValue, leafScript)
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
            public Leaf forLeaf(LeafReaderContext ctx) throws IOException {
                NumberSortScript leafScript = scriptSupplier.apply(ctx);
                return new Leaf(ctx) {
                    private final NumericDoubleValues docValues = getNumericDocValues(ctx, dMissingValue, leafScript);
                    private double docValue;

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
