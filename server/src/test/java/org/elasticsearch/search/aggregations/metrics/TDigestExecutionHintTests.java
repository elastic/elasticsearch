/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Collections;

public class TDigestExecutionHintTests extends ESTestCase {

    public void testParsing() {
        // Lower case.
        assertEquals(TDigestExecutionHint.DEFAULT, TDigestExecutionHint.parse(""));
        assertEquals(TDigestExecutionHint.DEFAULT, TDigestExecutionHint.parse("default"));
        assertEquals(TDigestExecutionHint.HIGH_ACCURACY, TDigestExecutionHint.parse("high_accuracy"));
        expectThrows(IllegalArgumentException.class, () -> TDigestExecutionHint.parse("no such hint"));

        // Upper case.
        assertEquals(TDigestExecutionHint.DEFAULT, TDigestExecutionHint.parse("DEFAULT"));
        assertEquals(TDigestExecutionHint.HIGH_ACCURACY, TDigestExecutionHint.parse("HIGH_ACCURACY"));
        expectThrows(IllegalArgumentException.class, () -> TDigestExecutionHint.parse("NO SUCH HINT"));
    }

    public void testDefaultValue() {
        assertEquals(TDigestExecutionHint.DEFAULT, TDigestExecutionHint.getDefaultValue());

        TDigestExecutionHint.setDefaultValue(TDigestExecutionHint.HIGH_ACCURACY.toString());
        assertEquals(TDigestExecutionHint.HIGH_ACCURACY, TDigestExecutionHint.getDefaultValue());

        TDigestExecutionHint hint = randomFrom(TDigestExecutionHint.values());
        TDigestExecutionHint.setDefaultValue(hint.toString());
        assertEquals(hint, TDigestExecutionHint.getDefaultValue());
    }

    private static TDigestExecutionHint writeToAndReadFrom(TDigestExecutionHint state) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            state.writeTo(out);
            try (
                StreamInput in = new NamedWriteableAwareStreamInput(
                    StreamInput.wrap(out.bytes().toBytesRef().bytes),
                    new NamedWriteableRegistry(Collections.emptyList())
                )
            ) {
                return TDigestExecutionHint.readFrom(in);

            }
        }
    }

    public void testSerialization() throws IOException {
        assertEquals(TDigestExecutionHint.DEFAULT, writeToAndReadFrom(TDigestExecutionHint.DEFAULT));
        assertEquals(TDigestExecutionHint.HIGH_ACCURACY, writeToAndReadFrom(TDigestExecutionHint.HIGH_ACCURACY));

        TDigestExecutionHint randomHint = randomFrom(TDigestExecutionHint.values());
        assertEquals(randomHint, writeToAndReadFrom(randomHint));
    }

    public void testUnexpectedSerializedValue() throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(-1);
            try (
                StreamInput in = new NamedWriteableAwareStreamInput(
                    StreamInput.wrap(out.bytes().toBytesRef().bytes),
                    new NamedWriteableRegistry(Collections.emptyList())
                )
            ) {
                expectThrows(IllegalStateException.class, () -> TDigestExecutionHint.readFrom(in));
            }
        }
    }
}
