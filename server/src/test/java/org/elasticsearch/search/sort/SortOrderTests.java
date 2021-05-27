/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.sort;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class SortOrderTests extends ESTestCase {

    /** Check that ordinals remain stable as we rely on them for serialisation. */
    public void testDistanceUnitNames() {
        assertEquals(0, SortOrder.ASC.ordinal());
        assertEquals(1, SortOrder.DESC.ordinal());
    }

    public void testReadWrite() throws Exception {
        for (SortOrder unit : SortOrder.values()) {
          try (BytesStreamOutput out = new BytesStreamOutput()) {
              unit.writeTo(out);
              try (StreamInput in = out.bytes().streamInput()) {
                  assertThat("Roundtrip serialisation failed.", SortOrder.readFromStream(in), equalTo(unit));
              }
          }
        }
    }

    public void testFromString() {
        for (SortOrder unit : SortOrder.values()) {
            assertThat("Roundtrip string parsing failed.", SortOrder.fromString(unit.toString()), equalTo(unit));
        }
    }
}
