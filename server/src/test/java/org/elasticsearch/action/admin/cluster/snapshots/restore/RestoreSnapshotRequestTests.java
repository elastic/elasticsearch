/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.snapshots.restore;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ToXContent;
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

import static org.hamcrest.Matchers.containsString;

public class RestoreSnapshotRequestTests extends AbstractWireSerializingTestCase<RestoreSnapshotRequest> {
    private RestoreSnapshotRequest randomState(RestoreSnapshotRequest instance) {
        if (randomBoolean()) {
            List<String> indices = new ArrayList<>();
            int count = randomInt(3) + 1;

            for (int i = 0; i < count; ++i) {
                indices.add(randomAlphaOfLength(randomInt(3) + 2));
            }

            instance.indices(indices);
        }

        if (randomBoolean()) {
            List<String> plugins = new ArrayList<>();
            int count = randomInt(3) + 1;

            for (int i = 0; i < count; ++i) {
                plugins.add(randomAlphaOfLength(randomInt(3) + 2));
            }

            instance.featureStates(plugins);
        }

        if (randomBoolean()) {
            instance.renamePattern(randomUnicodeOfLengthBetween(1, 100));
        }
        if (randomBoolean()) {
            instance.renameReplacement(randomUnicodeOfLengthBetween(1, 100));
        }
        instance.partial(randomBoolean());
        instance.includeAliases(randomBoolean());
        instance.quiet(randomBoolean());

        if (randomBoolean()) {
            Map<String, Object> indexSettings = new HashMap<>();
            int count = randomInt(3) + 1;

            for (int i = 0; i < count; ++i) {
                indexSettings.put(randomAlphaOfLengthBetween(2, 5), randomAlphaOfLengthBetween(2, 5));
            }
            instance.indexSettings(indexSettings);
        }

        instance.includeGlobalState(randomBoolean());

        if (randomBoolean()) {
            instance.indicesOptions(
                IndicesOptions.builder()
                    .concreteTargetOptions(new IndicesOptions.ConcreteTargetOptions(randomBoolean()))
                    .wildcardOptions(new IndicesOptions.WildcardOptions(randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean()))
                    .indexAbstractionOptions(
                        new IndicesOptions.IndexAbstractionOptions(instance.indicesOptions().ignoreAliases() == false, false, false)
                    )
                    .gatekeeperOptions(IndicesOptions.GatekeeperOptions.builder().allowSelectors(false).includeFailureIndices(true).build())
                    .build()
            );
        }

        instance.waitForCompletion(randomBoolean());

        if (randomBoolean()) {
            instance.masterNodeTimeout(randomTimeValue());
        }

        if (randomBoolean()) {
            instance.snapshotUuid(randomBoolean() ? null : randomAlphaOfLength(10));
        }

        if (randomBoolean()) {
            instance.encryptedDataPassword(new SecureString(randomAlphaOfLengthBetween(15, 30).toCharArray()));
        }

        return instance;
    }

    @Override
    protected RestoreSnapshotRequest createTestInstance() {
        return randomState(new RestoreSnapshotRequest(TEST_REQUEST_TIMEOUT, randomAlphaOfLength(5), randomAlphaOfLength(10)));
    }

    @Override
    protected Writeable.Reader<RestoreSnapshotRequest> instanceReader() {
        return RestoreSnapshotRequest::new;
    }

    @Override
    protected RestoreSnapshotRequest mutateInstance(RestoreSnapshotRequest instance) throws IOException {
        RestoreSnapshotRequest copy = copyInstance(instance);
        // ensure that at least one property is different
        copy.repository("copied-" + instance.repository());
        return randomState(copy);
    }

    public void testSource() throws IOException {
        RestoreSnapshotRequest original = createTestInstance();
        original.snapshotUuid(null); // cannot be set via the REST API
        original.quiet(false); // cannot be set via the REST API
        original.encryptedDataPassword(null); // deliberately omitted from toXContent
        XContentBuilder builder = original.toXContent(XContentFactory.jsonBuilder(), new ToXContent.MapParams(Collections.emptyMap()));
        Map<String, Object> map;
        try (
            XContentParser parser = XContentType.JSON.xContent()
                .createParser(NamedXContentRegistry.EMPTY, null, BytesReference.bytes(builder).streamInput())
        ) {
            map = parser.mapOrdered();
        }

        // we will only restore properties from the map that are contained in the request body. All other
        // properties are restored from the original (in the actual REST action this is restored from the
        // REST path and request parameters).
        RestoreSnapshotRequest processed = new RestoreSnapshotRequest(TEST_REQUEST_TIMEOUT, original.repository(), original.snapshot());
        processed.masterNodeTimeout(original.masterNodeTimeout());
        processed.waitForCompletion(original.waitForCompletion());

        processed.source(map);

        assertEquals(original, processed);
    }

    public void testEncryptionPasswordDroppedBeforeItsTransportVersion() throws IOException {
        RestoreSnapshotRequest original = new RestoreSnapshotRequest(TEST_REQUEST_TIMEOUT, "repo", "snap");
        original.encryptedDataPassword(new SecureString("a-perfectly-valid-password".toCharArray()));
        TransportVersion before = TransportVersionUtils.getPreviousVersion(TransportVersion.fromName("snapshot_encrypted_data_password"));
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setTransportVersion(before);
            original.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                in.setTransportVersion(before);
                assertNull(new RestoreSnapshotRequest(in).encryptedDataPassword());
            }
        }
    }

    public void testEncryptionPasswordValidation() {
        RestoreSnapshotRequest request = new RestoreSnapshotRequest(TEST_REQUEST_TIMEOUT, "repo", "snap");
        request.encryptedDataPassword(new SecureString("only-14-chars-".toCharArray()));
        ActionRequestValidationException validationException = request.validate();
        assertNotNull(validationException);
        assertThat(validationException.getMessage(), containsString("encrypted_data_password must be at least 15 characters"));

        request.encryptedDataPassword(new SecureString("exactly-15-char".toCharArray()));
        assertNull(request.validate());
    }

    public void testEncryptionPasswordSourceParsing() {
        RestoreSnapshotRequest request = new RestoreSnapshotRequest(TEST_REQUEST_TIMEOUT, "repo", "snap");
        request.source(Map.of("encrypted_data_password", "a-perfectly-valid-password"));
        assertEquals("a-perfectly-valid-password", request.encryptedDataPassword().toString());

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> request.source(Map.of("encrypted_data_password", 12345))
        );
        assertThat(e.getMessage(), containsString("malformed encrypted_data_password"));
    }

    public void testEncryptionPasswordIsNotSerialisedAsXContent() throws IOException {
        RestoreSnapshotRequest request = createTestInstance();
        request.encryptedDataPassword(new SecureString("a-perfectly-valid-password".toCharArray()));
        Map<String, Object> map = convertRequestToMap(request);
        assertFalse(map.containsKey("encrypted_data_password"));
    }

    public void testSkipOperatorOnlyWillNotBeSerialised() throws IOException {
        RestoreSnapshotRequest original = createTestInstance();
        assertFalse(original.skipOperatorOnlyState()); // default is false
        if (randomBoolean()) {
            original.skipOperatorOnlyState(true);
        }
        Map<String, Object> map = convertRequestToMap(original);
        // It is not serialised as xcontent
        assertFalse(map.containsKey("skip_operator_only"));

        // Xcontent is not affected by the value of skipOperatorOnlyState
        original.skipOperatorOnlyState(original.skipOperatorOnlyState() == false);
        assertEquals(map, convertRequestToMap(original));

        // Nor does it serialise to streamInput
        final BytesStreamOutput streamOutput = new BytesStreamOutput();
        original.writeTo(streamOutput);
        final RestoreSnapshotRequest deserialized = new RestoreSnapshotRequest(streamOutput.bytes().streamInput());
        assertFalse(deserialized.skipOperatorOnlyState());
    }

    public void testToStringWillIncludeSkipOperatorOnlyState() {
        RestoreSnapshotRequest original = createTestInstance();
        assertThat(original.toString(), containsString("skipOperatorOnlyState"));
    }

    public void testRenameReplacementNameTooLong() {
        RestoreSnapshotRequest request = createTestInstance();
        request.indices("b".repeat(255));
        request.renamePattern("b");
        request.renameReplacement("1".repeat(randomIntBetween(266, 10_000)));

        ActionRequestValidationException validation = request.validate();
        assertNotNull(validation);
        assertThat(validation.getMessage(), containsString("rename_replacement"));
    }

    private Map<String, Object> convertRequestToMap(RestoreSnapshotRequest request) throws IOException {
        XContentBuilder builder = request.toXContent(XContentFactory.jsonBuilder(), new ToXContent.MapParams(Collections.emptyMap()));
        try (
            XContentParser parser = XContentType.JSON.xContent()
                .createParser(NamedXContentRegistry.EMPTY, null, BytesReference.bytes(builder).streamInput())
        ) {
            return parser.mapOrdered();
        }
    }
}
