/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.cloud;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.Arrays;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class PersistedCloudCredentialTests extends ESTestCase {

    private static final String TEST_ID = "test-id-123";
    private static final byte[] TEST_PAYLOAD = new byte[29];

    static {
        Arrays.fill(TEST_PAYLOAD, (byte) 0xAB);
    }

    private static CloudCredentialEncryptedData testEncryptedData() {
        return new CloudCredentialEncryptedData(randomAlphaOfLength(22), randomByteArrayOfLength(29));
    }

    public void testNewInstanceStampsCurrentVersion() {
        var instance = new PersistedCloudCredential(TEST_ID, testEncryptedData());
        assertThat(instance.version(), is(equalTo(PersistedCloudCredential.CURRENT_VERSION)));
        assertThat(PersistedCloudCredential.CURRENT_VERSION, is(2));
    }

    public void testXContentRoundTripV2() throws IOException {
        CloudCredentialEncryptedData enc = testEncryptedData();
        var original = new PersistedCloudCredential(TEST_ID, enc);

        XContentBuilder builder = JsonXContent.contentBuilder();
        original.toXContent(builder, null);
        String json = Strings.toString(builder);

        try (XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, json)) {
            var parsed = PersistedCloudCredential.fromXContent(parser);
            assertThat(parsed, equalTo(original));
        }
    }

    public void testXContentRejectsV1() throws IOException {
        String v1Json = "{\"version\":1,\"id\":\"abc\",\"value\":\"supersecret\"}";
        try (XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, v1Json)) {
            // ConstructingObjectParser wraps constructor exceptions in XContentParseException
            XContentParseException ex = expectThrows(XContentParseException.class, () -> PersistedCloudCredential.fromXContent(parser));
            assertThat(ex.getCause().getMessage(), containsString("unsupported at-rest version [1]"));
        }
    }

    public void testWireV2RoundTrip() throws IOException {
        CloudCredentialEncryptedData enc = testEncryptedData();
        var original = new PersistedCloudCredential(TEST_ID, enc);

        BytesStreamOutput out = new BytesStreamOutput();
        out.setTransportVersion(PersistedCloudCredential.CLOUD_CREDENTIAL_ENCRYPTION);
        original.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        in.setTransportVersion(PersistedCloudCredential.CLOUD_CREDENTIAL_ENCRYPTION);
        var deserialized = new PersistedCloudCredential(in);
        assertThat(deserialized, equalTo(original));
    }

    public void testWireThrowsForOldPeer() {
        CloudCredentialEncryptedData enc = testEncryptedData();
        var original = new PersistedCloudCredential(TEST_ID, enc);

        BytesStreamOutput out = new BytesStreamOutput();
        // Use a version that does not support CLOUD_CREDENTIAL_ENCRYPTION
        out.setTransportVersion(
            org.elasticsearch.test.TransportVersionUtils.getPreviousVersion(PersistedCloudCredential.CLOUD_CREDENTIAL_ENCRYPTION)
        );
        IllegalStateException ex = expectThrows(IllegalStateException.class, () -> original.writeTo(out));
        assertThat(ex.getMessage(), containsString("cannot serialize PersistedCloudCredential to a peer that does not support"));
    }

    public void testToStringDoesNotExposeCiphertext() {
        var instance = new PersistedCloudCredential(TEST_ID, new CloudCredentialEncryptedData("key-1", TEST_PAYLOAD));
        String str = instance.toString();
        assertThat(str, containsString("key-1"));
        assertThat(str, not(containsString(Arrays.toString(TEST_PAYLOAD))));
    }
}
