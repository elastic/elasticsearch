/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.encryption.spi;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;

public class EncryptedDataTests extends AbstractXContentSerializingTestCase<EncryptedData> {

    @Override
    protected EncryptedData doParseInstance(XContentParser parser) throws IOException {
        return EncryptedData.fromXContent(parser);
    }

    @Override
    protected Writeable.Reader<EncryptedData> instanceReader() {
        return EncryptedData::new;
    }

    @Override
    protected EncryptedData createTestInstance() {
        return new EncryptedData(randomAlphaOfLengthBetween(5, 22), randomByteArrayOfLength(randomIntBetween(29, 256)));
    }

    @Override
    protected EncryptedData mutateInstance(EncryptedData instance) {
        if (randomBoolean()) {
            return new EncryptedData(randomValueOtherThan(instance.keyId(), () -> randomAlphaOfLengthBetween(5, 22)), instance.payload());
        } else {
            byte[] newPayload = randomValueOtherThanMany(
                p -> java.util.Arrays.equals(p, instance.payload()),
                () -> randomByteArrayOfLength(randomIntBetween(29, 256))
            );
            return new EncryptedData(instance.keyId(), newPayload);
        }
    }

    public void testToStringRedactsPayload() {
        EncryptedData data = createTestInstance();
        String str = data.toString();
        assertThat(str, containsString(data.keyId()));
        assertThat(str, containsString("::es_redacted::"));
    }

    /** The carrier must survive {@code writeGenericValue}/{@code readGenericValue} — the path ES|QL plan configs use. */
    public void testRidesGenericValueSerialization() throws IOException {
        EncryptedData original = createTestInstance();
        NamedWriteableRegistry registry = new NamedWriteableRegistry(List.of(EncryptedData.ENTRY));
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeGenericValue(original);
            try (StreamInput in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), registry)) {
                Object read = in.readGenericValue();
                assertThat(read, instanceOf(EncryptedData.class));
                assertEquals(original, read);
            }
        }
    }
}
