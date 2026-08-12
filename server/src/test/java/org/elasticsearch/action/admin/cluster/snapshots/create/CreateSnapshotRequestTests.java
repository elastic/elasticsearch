/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.snapshots.create;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ToXContent.MapParams;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.snapshots.SnapshotInfoTestUtils.randomUserMetadata;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class CreateSnapshotRequestTests extends ESTestCase {

    // tests creating XContent and parsing with source(Map) equivalency
    public void testToXContent() throws IOException {
        String repo = randomAlphaOfLength(5);
        String snap = randomAlphaOfLength(10);

        CreateSnapshotRequest original = new CreateSnapshotRequest(TEST_REQUEST_TIMEOUT, repo, snap);

        if (randomBoolean()) {
            List<String> indices = new ArrayList<>();
            int count = randomInt(3) + 1;

            for (int i = 0; i < count; ++i) {
                indices.add(randomAlphaOfLength(randomInt(3) + 2));
            }

            original.indices(indices);
        }

        if (randomBoolean()) {
            List<String> featureStates = new ArrayList<>();
            int count = randomInt(3) + 1;

            for (int i = 0; i < count; ++i) {
                featureStates.add(randomAlphaOfLength(randomInt(3) + 2));
            }

            original.featureStates(featureStates);
        }

        if (randomBoolean()) {
            original.partial(randomBoolean());
        }

        if (randomBoolean()) {
            original.includeGlobalState(randomBoolean());
        }

        if (randomBoolean()) {
            original.userMetadata(randomUserMetadata());
        }

        if (randomBoolean()) {
            boolean defaultResolveAliasForThisRequest = original.indicesOptions().ignoreAliases() == false;
            original.indicesOptions(
                IndicesOptions.builder()
                    .concreteTargetOptions(new IndicesOptions.ConcreteTargetOptions(randomBoolean()))
                    .wildcardOptions(new IndicesOptions.WildcardOptions(randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean()))
                    .indexAbstractionOptions(new IndicesOptions.IndexAbstractionOptions(defaultResolveAliasForThisRequest, false, false))
                    .gatekeeperOptions(IndicesOptions.GatekeeperOptions.builder().allowSelectors(false).includeFailureIndices(true).build())
                    .build()
            );
        }

        if (randomBoolean()) {
            original.waitForCompletion(randomBoolean());
        }

        if (randomBoolean()) {
            original.masterNodeTimeout(TimeValue.timeValueMinutes(1));
        }

        XContentBuilder builder = original.toXContent(XContentFactory.jsonBuilder(), new MapParams(Collections.emptyMap()));
        try (
            XContentParser parser = XContentType.JSON.xContent()
                .createParser(NamedXContentRegistry.EMPTY, null, BytesReference.bytes(builder).streamInput())
        ) {
            Map<String, Object> map = parser.mapOrdered();
            CreateSnapshotRequest processed = new CreateSnapshotRequest(
                TEST_REQUEST_TIMEOUT,
                (String) map.get("repository"),
                (String) map.get("snapshot")
            );
            processed.waitForCompletion(original.waitForCompletion());
            processed.masterNodeTimeout(original.masterNodeTimeout());
            processed.uuid(original.uuid());
            processed.source(map);

            assertEquals(original, processed);
        }
    }

    public void testSerializationRoundTripWithEncryptionPassword() throws IOException {
        CreateSnapshotRequest original = new CreateSnapshotRequest(TEST_REQUEST_TIMEOUT, randomAlphaOfLength(5), randomAlphaOfLength(10));
        if (randomBoolean()) {
            original.encryptedDataPassword(new SecureString(randomAlphaOfLengthBetween(15, 30).toCharArray()));
            if (randomBoolean()) {
                original.encryptedDataPasswordId(randomAlphaOfLengthBetween(3, 10));
            }
        }
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                CreateSnapshotRequest deserialized = new CreateSnapshotRequest(in);
                assertEquals(original, deserialized);
                assertEquals(original.hashCode(), deserialized.hashCode());
            }
        }
    }

    public void testEncryptionPasswordDroppedBeforeItsTransportVersion() throws IOException {
        CreateSnapshotRequest original = new CreateSnapshotRequest(TEST_REQUEST_TIMEOUT, "repo", "snap");
        original.encryptedDataPassword(new SecureString("a-perfectly-valid-password".toCharArray()));
        original.encryptedDataPasswordId("my-password-id");
        TransportVersion before = TransportVersionUtils.getPreviousVersion(TransportVersion.fromName("snapshot_encrypted_data_password"));
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setTransportVersion(before);
            original.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                in.setTransportVersion(before);
                CreateSnapshotRequest deserialized = new CreateSnapshotRequest(in);
                assertNull(deserialized.encryptedDataPassword());
                assertNull(deserialized.encryptedDataPasswordId());
            }
        }
    }

    public void testEncryptionPasswordValidation() {
        CreateSnapshotRequest request = new CreateSnapshotRequest(TEST_REQUEST_TIMEOUT, "repo", "snap");
        request.encryptedDataPassword(new SecureString("only-14-chars-".toCharArray()));
        ActionRequestValidationException validationException = request.validate();
        assertNotNull(validationException);
        assertThat(validationException.getMessage(), containsString("encrypted_data_password must be at least 15 characters"));

        request.encryptedDataPassword(new SecureString("exactly-15-char".toCharArray()));
        assertNull(request.validate());
    }

    public void testEncryptionPasswordIdRequiresPassword() {
        CreateSnapshotRequest request = new CreateSnapshotRequest(TEST_REQUEST_TIMEOUT, "repo", "snap");
        request.encryptedDataPasswordId("my-password-id");
        ActionRequestValidationException validationException = request.validate();
        assertNotNull(validationException);
        assertThat(validationException.getMessage(), containsString("encrypted_data_password_id requires encrypted_data_password"));

        request.encryptedDataPassword(new SecureString("a-perfectly-valid-password".toCharArray()));
        assertNull(request.validate());

        // the id remains optional when a password is set
        request.encryptedDataPasswordId(null);
        assertNull(request.validate());
    }

    public void testEncryptionPasswordSourceParsing() {
        CreateSnapshotRequest request = new CreateSnapshotRequest(TEST_REQUEST_TIMEOUT, "repo", "snap");
        request.source(Map.of("encrypted_data_password", "a-perfectly-valid-password"));
        assertEquals("a-perfectly-valid-password", request.encryptedDataPassword().toString());

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> request.source(Map.of("encrypted_data_password", 12345))
        );
        assertThat(e.getMessage(), containsString("malformed encrypted_data_password"));
    }

    public void testEncryptionPasswordIdSourceParsing() {
        CreateSnapshotRequest request = new CreateSnapshotRequest(TEST_REQUEST_TIMEOUT, "repo", "snap");
        request.source(Map.of("encrypted_data_password_id", "my-password-id"));
        assertEquals("my-password-id", request.encryptedDataPasswordId());

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> request.source(Map.of("encrypted_data_password_id", 12345))
        );
        assertThat(e.getMessage(), containsString("malformed encrypted_data_password_id"));
    }

    public void testEncryptionPasswordIsNotSerialisedAsXContent() throws IOException {
        CreateSnapshotRequest request = new CreateSnapshotRequest(TEST_REQUEST_TIMEOUT, "repo", "snap");
        request.encryptedDataPassword(new SecureString("a-perfectly-valid-password".toCharArray()));
        XContentBuilder builder = request.toXContent(XContentFactory.jsonBuilder(), new MapParams(Collections.emptyMap()));
        try (
            XContentParser parser = XContentType.JSON.xContent()
                .createParser(NamedXContentRegistry.EMPTY, null, BytesReference.bytes(builder).streamInput())
        ) {
            assertFalse(parser.mapOrdered().containsKey("encrypted_data_password"));
        }
    }

    public void testSizeCheck() {
        {
            Map<String, Object> simple = new HashMap<>();
            simple.put(randomAlphaOfLength(5), randomAlphaOfLength(25));
            assertNull(createSnapshotRequestWithMetadata(simple).validate());
        }

        {
            Map<String, Object> complex = new HashMap<>();
            Map<String, Object> nested = new HashMap<>();
            nested.put(randomAlphaOfLength(5), randomAlphaOfLength(5));
            nested.put(randomAlphaOfLength(6), randomAlphaOfLength(5));
            complex.put(randomAlphaOfLength(7), nested);
            assertNull(createSnapshotRequestWithMetadata(complex).validate());
        }

        {
            Map<String, Object> barelyFine = new HashMap<>();
            barelyFine.put(randomAlphaOfLength(512), randomAlphaOfLength(505));
            assertNull(createSnapshotRequestWithMetadata(barelyFine).validate());
        }

        {
            Map<String, Object> barelyTooBig = new HashMap<>();
            barelyTooBig.put(randomAlphaOfLength(512), randomAlphaOfLength(506));
            ActionRequestValidationException validationException = createSnapshotRequestWithMetadata(barelyTooBig).validate();
            assertNotNull(validationException);
            assertThat(validationException.validationErrors(), hasSize(1));
            assertThat(validationException.validationErrors().get(0), equalTo("metadata must be smaller than 1024 bytes, but was [1025]"));
        }

        {
            Map<String, Object> tooBigOnlyIfNestedFieldsAreIncluded = new HashMap<>();
            HashMap<Object, Object> nested = new HashMap<>();
            nested.put(randomAlphaOfLength(500), randomAlphaOfLength(500));
            tooBigOnlyIfNestedFieldsAreIncluded.put(randomAlphaOfLength(10), randomAlphaOfLength(10));
            tooBigOnlyIfNestedFieldsAreIncluded.put(randomAlphaOfLength(11), nested);

            ActionRequestValidationException validationException = createSnapshotRequestWithMetadata(tooBigOnlyIfNestedFieldsAreIncluded)
                .validate();
            assertNotNull(validationException);
            assertThat(validationException.validationErrors(), hasSize(1));
            assertThat(validationException.validationErrors().get(0), equalTo("metadata must be smaller than 1024 bytes, but was [1049]"));
        }
    }

    private CreateSnapshotRequest createSnapshotRequestWithMetadata(Map<String, Object> metadata) {
        return new CreateSnapshotRequest(TEST_REQUEST_TIMEOUT, randomAlphaOfLength(5), randomAlphaOfLength(5)).indices(
            randomAlphaOfLength(5)
        ).userMetadata(metadata);
    }
}
