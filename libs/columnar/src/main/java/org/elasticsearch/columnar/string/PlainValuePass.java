/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.columnar.substrate.ColumnIteratorWriter;

import java.io.IOException;

/**
 * A {@link ColumnIteratorWriter.Pass} that writes a plain string column's values: one entry per slot into
 * the value stream, one address per document into the addressing table, and one null-slot entry per null
 * slot. The pass accumulates whether values arrived in term order ({@link #sorted}) and the running value
 * address ({@link #valueAddress}), both read after the walk is done.
 */
final class PlainValuePass implements ColumnIteratorWriter.Pass<StringColumnValues> {

    private final ValueStream.Writer stream;
    private final AddressingWriter slots;
    private final NullSlotWriter nullSlots;
    private final BytesRef empty = new BytesRef(BytesRef.EMPTY_BYTES);
    private final BytesRefBuilder previous = new BytesRefBuilder();

    private boolean sorted = true;
    private boolean hasPrevious = false;
    private long valueAddress = 0;

    PlainValuePass(ValueStream.Writer stream, AddressingWriter slots, NullSlotWriter nullSlots) {
        this.stream = stream;
        this.slots = slots;
        this.nullSlots = nullSlots;
    }

    @Override
    public void accept(StringColumnValues cursor, int doc) throws IOException {
        slots.startDocument(valueAddress);
        for (int i = 0, count = cursor.valueCount(); i < count; i++) {
            cursor.nextValue();
            final BytesRef value = cursor.value();
            if (value == null) {
                // A null stores zero bytes; the null-slot table is the only thing that tells it from empty.
                // A null has no place in term order, so a column holding one is not one a search can bisect.
                sorted = false;
                nullSlots.recordNull(valueAddress);
                stream.add(empty);
            } else {
                if (sorted) {
                    if (hasPrevious && previous.get().compareTo(value) > 0) {
                        sorted = false;
                    } else {
                        previous.copyBytes(value);
                        hasPrevious = true;
                    }
                }
                stream.add(value);
            }
            valueAddress++;
        }
    }

    /** Whether all non-null values arrived in term order. */
    boolean sorted() {
        return sorted;
    }

    /** The total number of value slots written across all documents visited so far. */
    long valueAddress() {
        return valueAddress;
    }
}
