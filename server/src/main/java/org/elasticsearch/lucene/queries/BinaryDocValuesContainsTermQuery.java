/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene.queries;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.mapper.BinaryDocValuesFormat;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.simdvec.ESVectorUtil;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.index.mapper.MultiValuedBinaryDocValuesField.SeparateCount.COUNT_FIELD_SUFFIX;

/**
 * A query that matches documents whose binary doc values contain a specific term. It adds the whole-blob
 * {@code tryContainsIterator} fast path on top of the {@link AbstractBinaryDocValuesQuery} scanning path.
 */
public final class BinaryDocValuesContainsTermQuery extends AbstractBinaryDocValuesQuery {

    private final BytesRef containsTerm;

    BinaryDocValuesContainsTermQuery(String fieldName, BytesRef containsTerm, BinaryDocValuesFormat binaryFormat) {
        super(fieldName, bytes -> contains(bytes, containsTerm), binaryFormat);
        this.containsTerm = Objects.requireNonNull(containsTerm);
    }

    @Override
    protected DocIdSetIterator getDocIdSetIterator(LeafReaderContext context, float matchCost) throws IOException {
        final BinaryDocValues values = context.reader().getBinaryDocValues(fieldName);
        if (values == null) {
            return null;
        }
        // A payload blob is never a bare value, so the whole-blob scan below can never apply to one — and there is no
        // .counts companion to look up on the way to finding that out. super decodes the payload instead.
        if (binaryFormat != BinaryDocValuesFormat.COLUMNAR_PAYLOAD) {
            // tryContainsIterator scans the whole doc blob including the multi-valued length-prefix framing, so it is only
            // correct for single-valued fields where no length prefixes exist.
            final DocValuesSkipper countsSkipper = context.reader().getDocValuesSkipper(fieldName + COUNT_FIELD_SUFFIX);
            if ((countsSkipper == null || countsSkipper.maxValue() == 1)
                && values instanceof BlockLoader.OptionalColumnAtATimeReader direct) {
                final DocIdSetIterator containsIter = direct.tryContainsIterator(containsTerm);
                if (containsIter != null) {
                    return containsIter;
                }
            }
        }
        return super.getDocIdSetIterator(context, matchCost);
    }

    @Override
    protected float matchCost() {
        return 10;
    }

    @Override
    public String toString(String field) {
        return "BinaryDocValuesContainsTermQuery(fieldName=" + fieldName + ",containsTerm=" + containsTerm + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (sameClassAs(o) == false) {
            return false;
        }
        BinaryDocValuesContainsTermQuery that = (BinaryDocValuesContainsTermQuery) o;
        return Objects.equals(fieldName, that.fieldName)
            && Objects.equals(containsTerm, that.containsTerm)
            && binaryFormat == that.binaryFormat;
    }

    @Override
    public int hashCode() {
        return Objects.hash(classHash(), fieldName, containsTerm, binaryFormat);
    }

    public static boolean contains(BytesRef value, BytesRef term) {
        return contains(value.bytes, value.offset, value.length, term);
    }

    public static boolean contains(byte[] value, int offset, int length, BytesRef term) {
        return ESVectorUtil.contains(value, offset, length, term.bytes, term.offset, term.length);
    }
}
