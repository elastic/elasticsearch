/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class PlainSkipKindTests extends ESTestCase {

    public void testAKindIsFoundByTheIdItRecords() {
        final PlainSkipKind kind = PlainSkipKind.forWriting();
        assertNotEquals("zero is reserved for a column with no region", StringColumnMetadata.Plain.Skipper.NONE, kind.id());
        assertSame(kind, PlainSkipKind.forId(kind.id()));
    }

    public void testAnUnknownIdIsRejected() {
        final byte unknown = randomValueOtherThan(PlainSkipKind.forWriting().id(), () -> randomByte());
        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> PlainSkipKind.forId(unknown));
        assertEquals("unknown plain skip kind [" + unknown + "]", e.getMessage());
    }

    public void testARegionRecordsTheKindThatWroteIt() throws IOException {
        final BytesRef[][] docSlots = new BytesRef[][] { { new BytesRef("a") }, { new BytesRef("b") }, { new BytesRef("c") } };
        try (Directory dir = newDirectory(); PlainSkipFixture fixture = PlainSkipFixture.build(dir, docSlots, 1)) {
            assertEquals(PlainSkipKind.forWriting().id(), fixture.skipper.kind());
        }
    }
}
