/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.settings.secure;

import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.encryption.spi.EncryptedData;

import java.io.IOException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class SecureSettingsMetadataTests extends AbstractWireSerializingTestCase<SecureSettingsMetadata> {

    @Override
    protected Writeable.Reader<SecureSettingsMetadata> instanceReader() {
        return SecureSettingsMetadata::new;
    }

    @Override
    protected SecureSettingsMetadata createTestInstance() {
        Map<String, EncryptedData> settings = new HashMap<>();
        int count = randomIntBetween(0, 5);
        for (int i = 0; i < count; i++) {
            settings.put(randomAlphaOfLength(8) + "." + i, randomBoolean() ? null : randomEncryptedData());
        }
        return new SecureSettingsMetadata(settings);
    }

    @Override
    protected SecureSettingsMetadata mutateInstance(SecureSettingsMetadata instance) {
        Map<String, EncryptedData> settings = new HashMap<>(instance.settings());
        settings.put(randomAlphaOfLength(10) + ".new", randomEncryptedData());
        return new SecureSettingsMetadata(settings);
    }

    public void testXContentRoundTrip() throws IOException {
        SecureSettingsMetadata original = createTestInstance();
        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        ChunkedToXContent.wrapAsToXContent(original).toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        try (XContentParser parser = createParser(XContentType.JSON.xContent(), BytesReference.bytes(builder))) {
            assertThat(SecureSettingsMetadata.fromXContent(parser), equalTo(original));
        }
    }

    public void testOnlyPersistedToDisk() {
        assertThat(createTestInstance().context(), equalTo(EnumSet.of(Metadata.XContentContext.GATEWAY)));
    }

    public void testToStringOmitsValues() {
        EncryptedData data = randomEncryptedData();
        SecureSettingsMetadata metadata = new SecureSettingsMetadata(Map.of("a.b", data));
        assertThat(metadata.toString(), containsString("a.b"));
        assertThat(metadata.toString(), not(containsString(data.keyId())));
    }

    static EncryptedData randomEncryptedData() {
        return new EncryptedData(randomAlphaOfLength(12), randomByteArrayOfLength(randomIntBetween(1, 32)));
    }
}
