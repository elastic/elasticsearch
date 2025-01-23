/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.snapshots.create;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
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
                    .wildcardOptions(
                        new IndicesOptions.WildcardOptions(
                            randomBoolean(),
                            randomBoolean(),
                            randomBoolean(),
                            defaultResolveAliasForThisRequest,
                            randomBoolean()
                        )
                    )
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

    private CreateSnapshotRequest createSnapshotRequestWithMetadata(Map<String, Object> metadata) {
        return new CreateSnapshotRequest(TEST_REQUEST_TIMEOUT, randomAlphaOfLength(5), randomAlphaOfLength(5)).indices(
            randomAlphaOfLength(5)
        ).userMetadata(metadata);
    }
}
