/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.crypto;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.GenericNamedWriteable;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry.Entry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
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

    public void testNamedWriteableNameIsStable() {
        // Wire-format identity: the registered name is part of the on-wire shape; renaming it is a
        // breaking change for any cluster state that carries an EncryptedData carrier inside a
        // generic-value map. Pin it.
        assertEquals("encrypted_data", EncryptedData.NAMED_WRITEABLE_NAME);
        EncryptedData data = createTestInstance();
        assertEquals("encrypted_data", data.getWriteableName());
    }

    public void testRoundTripViaWriteGenericValue() throws IOException {
        // The whole point of implementing GenericNamedWriteable is so that EncryptedData can ride
        // inside any Map<String, Object> serialized via writeGenericValue — which is how
        // DataSourceSetting ships values on the wire. If this regresses, the storage encryption path
        // breaks even though the standalone Writeable round-trip continues to pass.
        NamedWriteableRegistry registry = new NamedWriteableRegistry(
            List.of(new Entry(GenericNamedWriteable.class, EncryptedData.NAMED_WRITEABLE_NAME, EncryptedData::new))
        );
        Map<String, Object> wire = new HashMap<>();
        wire.put("region", "us-east-1");
        wire.put("secret_access_key", createTestInstance());

        BytesStreamOutput out = new BytesStreamOutput();
        out.writeGenericValue(wire);

        try (StreamInput rawInput = out.bytes().streamInput(); StreamInput in = new NamedWriteableAwareStreamInput(rawInput, registry)) {
            @SuppressWarnings("unchecked")
            Map<String, Object> roundTripped = (Map<String, Object>) in.readGenericValue();
            assertThat(roundTripped.get("region"), equalTo("us-east-1"));
            assertThat(roundTripped.get("secret_access_key"), instanceOf(EncryptedData.class));
            assertEquals(wire.get("secret_access_key"), roundTripped.get("secret_access_key"));
        }
    }
}
