/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.slm;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.encryption.EncryptedData;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicyMetadataTests.randomSnapshotLifecyclePolicy;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;

/**
 * Covers the {@code encryptedPassword} and {@code encryptedPasswordId} fields of {@link SnapshotLifecyclePolicy}:
 * both must survive the wire and the persisted xcontent representations (gateway state, snapshot global state);
 * the password must never appear in API output while its id is returned by the API.
 */
public class SnapshotLifecyclePolicyEncryptedPasswordTests extends ESTestCase {

    private static final String ENCRYPTED_PASSWORD_FIELD = "\"encrypted_data_password\":";
    private static final String ENCRYPTED_PASSWORD_ID_FIELD = "\"encrypted_data_password_id\":";

    public void testWireRoundTrip() throws IOException {
        SnapshotLifecyclePolicy policy = randomPolicyWithPassword();
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            policy.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                SnapshotLifecyclePolicy deserialized = new SnapshotLifecyclePolicy(in);
                assertEquals(policy, deserialized);
                assertEquals(policy.hashCode(), deserialized.hashCode());
            }
        }
    }

    public void testEncryptedPasswordDroppedBeforeItsTransportVersion() throws IOException {
        SnapshotLifecyclePolicy policy = randomPolicyWithPassword();
        TransportVersion before = TransportVersionUtils.getPreviousVersion(TransportVersion.fromName("snapshot_encrypted_data_password"));
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setTransportVersion(before);
            policy.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                in.setTransportVersion(before);
                SnapshotLifecyclePolicy deserialized = new SnapshotLifecyclePolicy(in);
                assertNull(deserialized.getEncryptedPassword());
                assertNull(deserialized.getEncryptedPasswordId());
            }
        }
    }

    public void testEqualsDiffersOnPasswordAlone() {
        SnapshotLifecyclePolicy withPassword = randomPolicyWithPassword();
        SnapshotLifecyclePolicy without = withPassword.withEncryptedPassword(null);
        SnapshotLifecyclePolicy otherPassword = withPassword.withEncryptedPassword(randomEncryptedData());

        assertNotEquals(withPassword, without);
        assertNotEquals(withPassword, otherPassword);
        assertEquals(withPassword, withPassword.withEncryptedPassword(withPassword.getEncryptedPassword()));
        assertNull(without.getEncryptedPassword());
    }

    public void testClearingPasswordClearsIdButReplacingKeepsIt() {
        SnapshotLifecyclePolicy withPassword = randomPolicyWithPassword();
        assertNotNull(withPassword.getEncryptedPasswordId());

        assertNull(withPassword.withEncryptedPassword(null).getEncryptedPasswordId());
        assertEquals(
            withPassword.getEncryptedPasswordId(),
            withPassword.withEncryptedPassword(randomEncryptedData()).getEncryptedPasswordId()
        );
    }

    public void testApiContextOmitsEncryptedPasswordButReturnsItsId() {
        SnapshotLifecyclePolicy policy = randomPolicyWithPassword();
        // default params resolve to the API context
        String api = Strings.toString(policy);
        assertThat(api, not(containsString(ENCRYPTED_PASSWORD_FIELD)));
        assertThat(api, containsString(ENCRYPTED_PASSWORD_ID_FIELD));
        assertThat(api, containsString(policy.getEncryptedPasswordId()));
    }

    public void testPersistenceContextsRoundTripEncryptedPassword() throws IOException {
        SnapshotLifecyclePolicy policy = randomPolicyWithPassword();
        ToXContent.Params params = persistenceParams();

        XContentBuilder builder = XContentFactory.jsonBuilder();
        policy.toXContent(builder, params);
        try (
            XContentParser parser = XContentType.JSON.xContent()
                .createParser(NamedXContentRegistry.EMPTY, null, BytesReference.bytes(builder).streamInput())
        ) {
            SnapshotLifecyclePolicy parsed = SnapshotLifecyclePolicy.parse(parser, policy.getId());
            assertEquals(policy.getEncryptedPassword(), parsed.getEncryptedPassword());
            assertEquals(policy.getEncryptedPasswordId(), parsed.getEncryptedPasswordId());
            assertEquals(policy, parsed);
        }
    }

    /**
     * Regression test: {@link SnapshotLifecyclePolicyMetadata#toXContent} must pass its params through to the nested
     * policy — the params-dropping {@code XContentBuilder#field(String, ToXContent)} overload silently serialized the
     * policy in API context, losing the password from gateway and snapshot state.
     */
    public void testPolicyMetadataPropagatesContextParams() throws IOException {
        SnapshotLifecyclePolicyMetadata metadata = SnapshotLifecyclePolicyMetadata.builder()
            .setPolicy(randomPolicyWithPassword())
            .setVersion(randomNonNegativeLong())
            .setModifiedDate(randomNonNegativeLong())
            .build();

        assertThat(Strings.toString(metadata), not(containsString(ENCRYPTED_PASSWORD_FIELD)));

        XContentBuilder builder = XContentFactory.jsonBuilder();
        metadata.toXContent(builder, persistenceParams());
        assertThat(Strings.toString(builder), containsString(ENCRYPTED_PASSWORD_FIELD));
    }

    private static ToXContent.Params persistenceParams() {
        return new ToXContent.MapParams(
            Map.of(Metadata.CONTEXT_MODE_PARAM, randomFrom(Metadata.XContentContext.GATEWAY, Metadata.XContentContext.SNAPSHOT).toString())
        );
    }

    private static SnapshotLifecyclePolicy randomPolicyWithPassword() {
        SnapshotLifecyclePolicy base = randomSnapshotLifecyclePolicy(randomAlphaOfLength(6));
        return new SnapshotLifecyclePolicy(
            base.getId(),
            base.getName(),
            base.getSchedule(),
            base.getRepository(),
            base.getConfig(),
            base.getRetentionPolicy(),
            base.getUnhealthyIfNoSnapshotWithin(),
            randomEncryptedData(),
            randomAlphaOfLengthBetween(3, 12)
        );
    }

    private static EncryptedData randomEncryptedData() {
        return new EncryptedData(randomAlphaOfLengthBetween(3, 12), randomByteArrayOfLength(randomIntBetween(1, 64)));
    }
}
