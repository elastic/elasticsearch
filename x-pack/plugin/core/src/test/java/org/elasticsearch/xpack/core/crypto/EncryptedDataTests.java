/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.crypto;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;

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
}
