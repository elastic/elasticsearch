/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.protocol.shared;

import org.apache.http.client.entity.DeflateInputStream;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class SqlDataInputOutputTests extends ESTestCase {
    public void testSmallString() throws IOException {
        assertRoundTripString("t");
        assertRoundTripString("test");
        assertRoundTripString(randomAlphaOfLengthBetween(500, 1000));
    }

    public void testLargeAscii() throws IOException {
        assertRoundTripString(randomAlphaOfLengthBetween(65535, 655350));
    }

    public void testUnicode() throws IOException {
        assertRoundTripString(randomRealisticUnicodeOfLengthBetween(65535 / 3, 65535));
        assertRoundTripString(randomRealisticUnicodeOfLengthBetween(65535, 655350));
    }

    /**
     * Round trip a string using {@link SqlDataOutput#writeUTF(String)}
     * and {@link SqlDataInput#readUTF()}.
     */
    private void assertRoundTripString(String string) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            SqlDataOutput sout = new SqlDataOutput(new DataOutputStream(out), 0);
            sout.writeUTF(string);
            try (StreamInput in = out.bytes().streamInput()) {
                SqlDataInput sin = new SqlDataInput(new DataInputStream(in), 0);
                assertEquals(string, sin.readUTF());
            }
        }
    }
}
