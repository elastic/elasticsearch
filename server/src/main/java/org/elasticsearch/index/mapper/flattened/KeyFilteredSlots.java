/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.flattened;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.mapper.MultiValuedBinaryDocValuesField;

import java.io.IOException;

/**
 * Doc values of a {@code flattened} field in {@link MultiValuedBinaryDocValuesField.KeyedArrayOrderInlineNull} that hold slots of
 * keys a reader must not see, such as the unmapped fields that field-level security hides. They return a copy without those slots
 * to any reader, but a reader that decodes the slots itself reads them as they are stored and skips the keys {@link #grants} rejects.
 */
public interface KeyFilteredSlots {

    /** Positions on {@code doc} as stored, and returns whether it has any slot. */
    boolean advanceExactStored(int doc) throws IOException;

    /** The slots of the current document as stored, granted or not. */
    BytesRef storedSlots() throws IOException;

    /** The number of {@link #storedSlots}. */
    int storedSlotCount() throws IOException;

    /** Whether a reader may see the slots of {@code key}. */
    boolean grants(String key);
}
