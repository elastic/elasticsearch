/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.spatial.index.fielddata.plain;

import org.apache.lucene.geo.XYEncodingUtils;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.util.Accountable;
import org.elasticsearch.xpack.spatial.index.fielddata.MultiPointValues;
import org.elasticsearch.xpack.spatial.common.CartesianPoint;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

final class CartesianPointDVLeafFieldData extends AbstractLeafPointFieldData {
    private final LeafReader reader;
    private final String fieldName;

    CartesianPointDVLeafFieldData(LeafReader reader, String fieldName) {
        super();
        this.reader = reader;
        this.fieldName = fieldName;
    }

    @Override
    public long ramBytesUsed() {
        return 0; // not exposed by lucene
    }

    @Override
    public Collection<Accountable> getChildResources() {
        return Collections.emptyList();
    }

    @Override
    public void close() {
        // noop
    }

    @Override
    public MultiPointValues getPointValues() {
        try {
            final SortedNumericDocValues numericValues = DocValues.getSortedNumeric(reader, fieldName);
            return new MultiPointValues() {

                final CartesianPoint point = new CartesianPoint();

                @Override
                public boolean advanceExact(int doc) throws IOException {
                    return numericValues.advanceExact(doc);
                }

                @Override
                public int docValueCount() {
                    return numericValues.docValueCount();
                }

                @Override
                public CartesianPoint nextValue() throws IOException {
                    final long encoded = numericValues.nextValue();
                    point.reset(XYEncodingUtils.decode((int) (encoded >>> 32)),
                                XYEncodingUtils.decode((int) encoded));
                    return point;
                }
            };
        } catch (IOException e) {
            throw new IllegalStateException("Cannot load doc values", e);
        }
    }
}
