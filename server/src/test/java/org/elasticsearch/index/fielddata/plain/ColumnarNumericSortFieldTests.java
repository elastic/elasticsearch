/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.fielddata.plain;

import org.apache.lucene.index.SortFieldProvider;
import org.apache.lucene.search.SortField;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class ColumnarNumericSortFieldTests extends ESTestCase {

    /** A segment sorted by the field records it in its segment info, which is read back through the SPI-registered provider. */
    public void testProviderRoundTrip() throws IOException {
        Long missing = randomBoolean() ? null : randomFrom(Long.MIN_VALUE, Long.MAX_VALUE, randomLong());
        ColumnarNumericSortField original = new ColumnarNumericSortField(
            randomAlphaOfLength(5),
            SortField.Type.LONG,
            randomBoolean(),
            randomBoolean(),
            missing
        );
        SortFieldProvider provider = SortFieldProvider.forName(ColumnarNumericSortField.PROVIDER_NAME);
        ByteBuffersDataOutput out = new ByteBuffersDataOutput();
        provider.writeSortField(original, out);
        SortField restored = provider.readSortField(out.toDataInput());
        assertEquals(original, restored);
        assertEquals(SortField.Type.LONG, restored.getType());
        assertEquals(original.maxMode(), ((ColumnarNumericSortField) restored).maxMode());
    }

    /** Only long fields are stored with ColumNAR so far. */
    public void testOnlyLongIsSupported() {
        SortField.Type type = randomValueOtherThan(SortField.Type.LONG, () -> randomFrom(SortField.Type.values()));
        expectThrows(IllegalArgumentException.class, () -> new ColumnarNumericSortField("field", type, false, false, null));
    }
}
