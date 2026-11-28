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
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.snapshots.SnapshotEncryptedData;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ToXContent;
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
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

public class CreateSnapshotRequestTests extends ESTestCase {

    private static final String PASSWORD = "correct-horse-battery-staple";

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

    public void testEncryptedDataFromSource() {
        assumeTrue("encrypted_data is only parsed with the feature flag", SnapshotEncryptedData.FEATURE_FLAG.isEnabled());
        final CreateSnapshotRequest request = new CreateSnapshotRequest(TEST_REQUEST_TIMEOUT, "repo", "snap").source(
            Map.of("encrypted_data", Map.of("type", "password", "password", PASSWORD, "password_id", "pw-1"))
        );
        assertThat(request.encryptedData().type(), equalTo(SnapshotEncryptedData.TYPE_PASSWORD));
        assertThat(request.encryptedData().password().toString(), equalTo(PASSWORD));
        assertThat(request.encryptedData().passwordId(), equalTo("pw-1"));
        assertNull(request.validate());

        final ActionRequestValidationException e = request.includeGlobalState(false).validate();
        assertNotNull(e);
        assertThat(e.validationErrors(), contains("encrypted_data requires include_global_state to be true"));
    }

    public void testEncryptedDataPasswordNotInToXContentOrToString() throws IOException {
        final CreateSnapshotRequest request = requestWithEncryptedData();
        final String xContent = Strings.toString(request.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS));
        assertThat(xContent, containsString("pw-1"));
        assertThat(xContent, not(containsString(PASSWORD)));
        assertThat(request.toString(), not(containsString(PASSWORD)));
    }

    public void testEncryptedDataSerialization() throws IOException {
        final CreateSnapshotRequest request = requestWithEncryptedData();
        final NamedWriteableRegistry registry = new NamedWriteableRegistry(List.of());
        assertEquals(request, copyWriteable(request, registry, CreateSnapshotRequest::new));

        final TransportVersion previous = TransportVersionUtils.randomVersionNotSupporting(SnapshotEncryptedData.TRANSPORT_VERSION);
        assertNull(copyWriteable(request, registry, CreateSnapshotRequest::new, previous).encryptedData());
    }

    private static CreateSnapshotRequest requestWithEncryptedData() {
        return new CreateSnapshotRequest(TEST_REQUEST_TIMEOUT, "repo", "snap").encryptedData(
            new SnapshotEncryptedData(SnapshotEncryptedData.TYPE_PASSWORD, new SecureString(PASSWORD.toCharArray()), "pw-1")
        );
    }

    private CreateSnapshotRequest createSnapshotRequestWithMetadata(Map<String, Object> metadata) {
        return new CreateSnapshotRequest(TEST_REQUEST_TIMEOUT, randomAlphaOfLength(5), randomAlphaOfLength(5)).indices(
            randomAlphaOfLength(5)
        ).userMetadata(metadata);
    }
}
