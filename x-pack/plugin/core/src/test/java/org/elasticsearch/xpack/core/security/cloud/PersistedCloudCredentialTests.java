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
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xcontent.XContentBuilder;
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

    public void testEncryptedInstanceStampsCurrentVersion() {
        var instance = new PersistedCloudCredential(TEST_ID, testEncryptedData());
        assertThat(instance.version(), is(equalTo(PersistedCloudCredential.CURRENT_VERSION)));
        assertThat(PersistedCloudCredential.CURRENT_VERSION, is(2));
        assertThat(instance.encrypted(), is(not(equalTo(null))));
        assertThat(instance.internalApiKey(), is(equalTo(null)));
    }

    public void testPlaintextInstanceStampsV1() {
        var instance = PersistedCloudCredential.plaintext(TEST_ID, new SecureString("secret".toCharArray()));
        assertThat(instance.version(), is(equalTo(PersistedCloudCredential.PLAINTEXT_VERSION)));
        assertThat(instance.internalApiKey().toString(), is(equalTo("secret")));
        assertThat(instance.encrypted(), is(equalTo(null)));
    }

    public void testXContentRoundTripV2() throws IOException {
        var original = new PersistedCloudCredential(TEST_ID, testEncryptedData());
        assertThat(xContentRoundTrip(original), equalTo(original));
    }

    public void testXContentRoundTripV1() throws IOException {
        var original = PersistedCloudCredential.plaintext(TEST_ID, new SecureString("supersecret".toCharArray()));
        assertThat(xContentRoundTrip(original), equalTo(original));
    }

    public void testXContentReadsLegacyV1Document() throws IOException {
        String v1Json = "{\"version\":1,\"id\":\"abc\",\"value\":\"supersecret\"}";
        try (XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, v1Json)) {
            var parsed = PersistedCloudCredential.fromXContent(parser);
            assertThat(parsed.version(), is(1));
            assertThat(parsed.id(), is("abc"));
            assertThat(parsed.internalApiKey().toString(), is("supersecret"));
        }
    }

    public void testWireV2RoundTrip() throws IOException {
        var original = new PersistedCloudCredential(TEST_ID, testEncryptedData());

        BytesStreamOutput out = new BytesStreamOutput();
        out.setTransportVersion(PersistedCloudCredential.CLOUD_CREDENTIAL_ENCRYPTION);
        original.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        in.setTransportVersion(PersistedCloudCredential.CLOUD_CREDENTIAL_ENCRYPTION);
        assertThat(new PersistedCloudCredential(in), equalTo(original));
    }

    public void testWireV1RoundTripToAnyPeer() throws IOException {
        var original = PersistedCloudCredential.plaintext(TEST_ID, new SecureString("supersecret".toCharArray()));

        BytesStreamOutput out = new BytesStreamOutput();
        // v1 serializes even to a peer that predates CLOUD_CREDENTIAL_ENCRYPTION
        out.setTransportVersion(TransportVersionUtils.getPreviousVersion(PersistedCloudCredential.CLOUD_CREDENTIAL_ENCRYPTION));
        original.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        in.setTransportVersion(TransportVersionUtils.getPreviousVersion(PersistedCloudCredential.CLOUD_CREDENTIAL_ENCRYPTION));
        assertThat(new PersistedCloudCredential(in), equalTo(original));
    }

    public void testWireV2ThrowsForOldPeer() {
        var original = new PersistedCloudCredential(TEST_ID, testEncryptedData());

        BytesStreamOutput out = new BytesStreamOutput();
        out.setTransportVersion(TransportVersionUtils.getPreviousVersion(PersistedCloudCredential.CLOUD_CREDENTIAL_ENCRYPTION));
        IllegalStateException ex = expectThrows(IllegalStateException.class, () -> original.writeTo(out));
        assertThat(ex.getMessage(), containsString("cannot write encrypted cloud credential to a peer at transport version"));
    }

    public void testToStringDoesNotExposeCiphertext() {
        var instance = new PersistedCloudCredential(TEST_ID, new CloudCredentialEncryptedData("key-1", TEST_PAYLOAD));
        String str = instance.toString();
        assertThat(str, containsString("key-1"));
        assertThat(str, not(containsString(Arrays.toString(TEST_PAYLOAD))));
    }

    public void testToStringDoesNotExposePlaintext() {
        var instance = PersistedCloudCredential.plaintext(TEST_ID, new SecureString("supersecret".toCharArray()));
        assertThat(instance.toString(), not(containsString("supersecret")));
        assertThat(instance.toString(), containsString("::es_redacted::"));
    }

    private static PersistedCloudCredential xContentRoundTrip(PersistedCloudCredential original) throws IOException {
        XContentBuilder builder = JsonXContent.contentBuilder();
        original.toXContent(builder, null);
        String json = Strings.toString(builder);
        try (XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, json)) {
            return PersistedCloudCredential.fromXContent(parser);
        }
    }
}
