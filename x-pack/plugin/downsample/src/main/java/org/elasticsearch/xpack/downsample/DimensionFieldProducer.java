/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.apache.lucene.internal.hppc.IntArrayList;
import org.elasticsearch.index.fielddata.FormattedDocValues;

import java.io.IOException;
import java.util.Objects;

/**
 * The dimension field producer is effectively a last value field producer that performs some extra validations when assertions are enabled.
 * It checks:
 * - that a tsid is only collected once, and
 * - that all TSIDs that are being collected for a round have the same value.
 * Important note: This class assumes that field values are collected and sorted by descending order by time
 */
public class DimensionFieldProducer extends LastValueFieldProducer {

    DimensionFieldProducer(final String name) {
        super(name, false);
    }

    void collectOnce(final Object value) {
        assert isEmpty;
        Objects.requireNonNull(value);
        this.lastValue = value;
        this.isEmpty = false;
    }

    /**
     * This is an expensive check that slows down downsampling significantly.
     * Given that index is sorted by tsid as a primary key, this shouldn't really happen.
     */
    boolean validate(FormattedDocValues docValues, IntArrayList buffer) throws IOException {
        for (int i = 0; i < buffer.size(); i++) {
            int docId = buffer.get(i);
            if (docValues.advanceExact(docId)) {
                int docValueCount = docValues.docValueCount();
                for (int j = 0; j < docValueCount; j++) {
                    var value = docValues.nextValue();
                    assert value.equals(this.lastValue) != false
                        : "Dimension value changed without tsid change [" + value + "] != [" + this.lastValue + "]";
                }
            }
        }

        return true;
    }

    @Override
    public void collect(FormattedDocValues docValues, IntArrayList docIdBuffer) throws IOException {
        if (isEmpty() == false) {
            assert validate(docValues, docIdBuffer);
            return;
        }

        for (int i = 0; i < docIdBuffer.size(); i++) {
            int docId = docIdBuffer.get(i);
            if (docValues.advanceExact(docId) == false) {
                continue;
            }
            int docValueCount = docValues.docValueCount();
            for (int j = 0; j < docValueCount; j++) {
                collectOnce(docValues.nextValue());
            }
            // Only need to record one dimension value from one document, within in the same tsid-and-time-interval bucket values are the
            // same.
            return;
        }
    }
}
