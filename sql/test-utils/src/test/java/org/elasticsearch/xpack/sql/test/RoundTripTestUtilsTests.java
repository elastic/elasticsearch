/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.test;

import org.elasticsearch.test.ESTestCase;

import java.io.DataInput;
import java.io.IOException;

import static org.elasticsearch.xpack.sql.test.RoundTripTestUtils.assertRoundTrip;
import static org.hamcrest.Matchers.startsWith;

public class RoundTripTestUtilsTests extends ESTestCase {
    public void testAssertRoundTrip() throws IOException {
        // Should pass
        assertRoundTrip(randomAlphaOfLength(5), (str, out) -> out.writeUTF(str), DataInput::readUTF);

        // Should fail because we have trailing stuff
        AssertionError e = expectThrows(AssertionError.class, () -> assertRoundTrip(randomAlphaOfLength(5), (str, out) -> {
            out.writeUTF(str);
            out.writeInt(randomInt());
        }, DataInput::readUTF));
        assertEquals("should have emptied the stream expected:<0> but was:<4>", e.getMessage());

        // Should fail because we read the wrong string
        e = expectThrows(AssertionError.class,  () -> assertRoundTrip(randomAlphaOfLength(5),
                (str, out) -> out.writeUTF(str), in -> in.readUTF() + "wrong"));
        assertThat(e.getMessage(), startsWith("expected:<"));
    }
}
