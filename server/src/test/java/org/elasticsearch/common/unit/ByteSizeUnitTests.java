/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.unit;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.elasticsearch.common.unit.ByteSizeUnit.BYTES;
import static org.elasticsearch.common.unit.ByteSizeUnit.GB;
import static org.elasticsearch.common.unit.ByteSizeUnit.KB;
import static org.elasticsearch.common.unit.ByteSizeUnit.MB;
import static org.elasticsearch.common.unit.ByteSizeUnit.PB;
import static org.elasticsearch.common.unit.ByteSizeUnit.TB;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class ByteSizeUnitTests extends ESTestCase {

    public void testBytes() {
        assertThat(BYTES.toBytes(1), equalTo(1L));
        assertThat(BYTES.toKB(1024), equalTo(1L));
        assertThat(BYTES.toMB(1024 * 1024), equalTo(1L));
        assertThat(BYTES.toGB(1024 * 1024 * 1024), equalTo(1L));
    }

    public void testKB() {
        assertThat(KB.toBytes(1), equalTo(1024L));
        assertThat(KB.toKB(1), equalTo(1L));
        assertThat(KB.toMB(1024), equalTo(1L));
        assertThat(KB.toGB(1024 * 1024), equalTo(1L));
    }

    public void testMB() {
        assertThat(MB.toBytes(1), equalTo(1024L * 1024));
        assertThat(MB.toKB(1), equalTo(1024L));
        assertThat(MB.toMB(1), equalTo(1L));
        assertThat(MB.toGB(1024), equalTo(1L));
    }

    public void testGB() {
        assertThat(GB.toBytes(1), equalTo(1024L * 1024 * 1024));
        assertThat(GB.toKB(1), equalTo(1024L * 1024));
        assertThat(GB.toMB(1), equalTo(1024L));
        assertThat(GB.toGB(1), equalTo(1L));
    }

    public void testTB() {
        assertThat(TB.toBytes(1), equalTo(1024L * 1024 * 1024 * 1024));
        assertThat(TB.toKB(1), equalTo(1024L * 1024 * 1024));
        assertThat(TB.toMB(1), equalTo(1024L * 1024));
        assertThat(TB.toGB(1), equalTo(1024L));
        assertThat(TB.toTB(1), equalTo(1L));
    }

    public void testPB() {
        assertThat(PB.toBytes(1), equalTo(1024L * 1024 * 1024 * 1024 * 1024));
        assertThat(PB.toKB(1), equalTo(1024L * 1024 * 1024 * 1024));
        assertThat(PB.toMB(1), equalTo(1024L * 1024 * 1024));
        assertThat(PB.toGB(1), equalTo(1024L * 1024));
        assertThat(PB.toTB(1), equalTo(1024L));
        assertThat(PB.toPB(1), equalTo(1L));
    }

    public void testToString() {
        int v = randomIntBetween(1, 1023);
        assertThat(new ByteSizeValue(PB.toBytes(v)).toString(), equalTo(v + "pb"));
        assertThat(new ByteSizeValue(TB.toBytes(v)).toString(), equalTo(v + "tb"));
        assertThat(new ByteSizeValue(GB.toBytes(v)).toString(), equalTo(v + "gb"));
        assertThat(new ByteSizeValue(MB.toBytes(v)).toString(), equalTo(v + "mb"));
        assertThat(new ByteSizeValue(KB.toBytes(v)).toString(), equalTo(v + "kb"));
        assertThat(new ByteSizeValue(BYTES.toBytes(v)).toString(), equalTo(v + "b"));
    }

    public void testSerialization() throws IOException {
        for (ByteSizeUnit unit : ByteSizeUnit.values()) {
            try (BytesStreamOutput out = new BytesStreamOutput()) {
                unit.writeTo(out);

                try (StreamInput in = out.bytes().streamInput()) {
                    ByteSizeUnit deserialized = ByteSizeUnit.readFrom(in);
                    assertEquals(unit, deserialized);
                }
            }
        }
    }

    public void testFromUnknownId() throws IOException {
        final byte randomId = (byte) randomIntBetween(ByteSizeUnit.values().length + 1, 100);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> ByteSizeUnit.fromId(randomId));
        assertThat(e.getMessage(), containsString("No byte size unit found for id [" + String.valueOf(randomId) + "]"));
    }
}
