/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.common.io;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.lessThan;

public class SqlStreamTests extends ESTestCase {

    public void testWriteAndRead() throws IOException {
        BytesRef payload = new BytesRef(randomByteArrayOfLength(randomIntBetween(10, 1000)));

        SqlStreamOutput out = SqlStreamOutput.create(TransportVersion.current(), randomZone());
        out.writeBytesRef(payload);
        out.close();
        String encoded = out.streamAsString();

        SqlStreamInput in = SqlStreamInput.fromString(encoded, new NamedWriteableRegistry(List.of()), TransportVersion.current());
        BytesRef read = in.readBytesRef();

        assertArrayEquals(payload.bytes, read.bytes);
    }

    public void testPayloadIsCompressed() throws IOException {
        SqlStreamOutput out = SqlStreamOutput.create(TransportVersion.current(), randomZone());
        byte[] payload = new byte[1000];
        Arrays.fill(payload, (byte) 0);
        out.write(payload);
        out.close();

        String result = out.streamAsString();
        assertThat(result.length(), lessThan(1000));
    }

    public void testOldCursorProducesVersionMismatchError() {
        SqlIllegalArgumentException ex = expectThrows(
            SqlIllegalArgumentException.class,
            () -> SqlStreamInput.fromString(
                // some cursor produced by ES 7.15.1
                "97S0AwFaAWMBCHRlc3RfZW1whgEBAQljb21wb3NpdGUHZ3JvdXBieQEDbWF4CDJkMTBjNGJhAAD/AQls"
                    + "YW5ndWFnZXMAAAD/AAD/AQAIYmRlZjg4ZTUBBmdlbmRlcgAAAQAAAQEKAQhiZGVmODhlNf8AAgEAAAAA"
                    + "AP////8PAAAAAAAAAAAAAAAAAVoDAAICAAAAAAAAAAAKAP////8PAgFtCDJkMTBjNGJhBXZhbHVlAAEE"
                    + "QllURQFrCGJkZWY4OGU1AAABAwA=",
                new NamedWriteableRegistry(List.of()),
                TransportVersion.current()
            )
        );

        assertThat(ex.getMessage(), containsString("Unsupported cursor version [7150199], expected [" + TransportVersion.current() + "]"));
    }

    public void testVersionCanBeReadByOldNodes() throws IOException {
        TransportVersion version = TransportVersions.V_8_1_0;
        SqlStreamOutput out = SqlStreamOutput.create(version, randomZone());
        out.writeString("payload");
        out.close();
        String encoded = out.streamAsString();

        byte[] bytes = Base64.getDecoder().decode(encoded);
        InputStreamStreamInput in = new InputStreamStreamInput(new ByteArrayInputStream(bytes));

        assertEquals(version, TransportVersion.readVersion(in));
    }

}
