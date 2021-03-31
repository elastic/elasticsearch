/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.unsignedlong;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.index.fielddata.FormattedDocValues;
import org.elasticsearch.index.fielddata.LeafNumericFieldData;
import org.elasticsearch.index.fielddata.NumericDoubleValues;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.search.DocValueFormat;

import java.io.IOException;

import static org.elasticsearch.xpack.unsignedlong.UnsignedLongFieldMapper.sortableSignedLongToUnsigned;

public class UnsignedLongLeafFieldData implements LeafNumericFieldData {
    private final LeafNumericFieldData signedLongFD;

    UnsignedLongLeafFieldData(LeafNumericFieldData signedLongFD) {
        this.signedLongFD = signedLongFD;
    }

    @Override
    public SortedNumericDocValues getLongValues() {
        return signedLongFD.getLongValues();
    }

    @Override
    public SortedNumericDoubleValues getDoubleValues() {
        final SortedNumericDocValues values = signedLongFD.getLongValues();
        final NumericDocValues singleValues = DocValues.unwrapSingleton(values);
        if (singleValues != null) {
            return FieldData.singleton(new NumericDoubleValues() {
                @Override
                public boolean advanceExact(int doc) throws IOException {
                    return singleValues.advanceExact(doc);
                }

                @Override
                public double doubleValue() throws IOException {
                    return convertUnsignedLongToDouble(singleValues.longValue());
                }
            });
        } else {
            return new SortedNumericDoubleValues() {

                @Override
                public boolean advanceExact(int target) throws IOException {
                    return values.advanceExact(target);
                }

                @Override
                public double nextValue() throws IOException {
                    return convertUnsignedLongToDouble(values.nextValue());
                }

                @Override
                public int docValueCount() {
                    return values.docValueCount();
                }
            };
        }
    }

    @Override
    public ScriptDocValues<?> getScriptValues() {
        // TODO: add support for scripts
        throw new UnsupportedOperationException("Using unsigned_long in scripts is currently not supported!");
    }

    @Override
    public SortedBinaryDocValues getBytesValues() {
        return FieldData.toString(getDoubleValues());
    }

    @Override
    public long ramBytesUsed() {
        return signedLongFD.ramBytesUsed();
    }

    @Override
    public void close() {
        signedLongFD.close();
    }

    @Override
    public FormattedDocValues getFormattedValues(DocValueFormat format) {
        SortedNumericDocValues values = getLongValues();
        return new FormattedDocValues() {
            @Override
            public boolean advanceExact(int docId) throws IOException {
                return values.advanceExact(docId);
            }

            @Override
            public int docValueCount() {
                return values.docValueCount();
            }

            @Override
            public Object nextValue() throws IOException {
                return format.format(values.nextValue());
            }
        };
    }

    static double convertUnsignedLongToDouble(long value) {
        if (value < 0L) {
            return sortableSignedLongToUnsigned(value); // add 2 ^ 63
        } else {
            // add 2 ^ 63 as a double to make sure there is no overflow and final result is positive
            return 0x1.0p63 + value;
        }
    }
}
