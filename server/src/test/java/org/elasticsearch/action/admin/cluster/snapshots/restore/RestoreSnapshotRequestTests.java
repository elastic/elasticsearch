/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.snapshots.restore;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
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
                    .wildcardOptions(
                        new IndicesOptions.WildcardOptions(
                            randomBoolean(),
                            randomBoolean(),
                            randomBoolean(),
                            instance.indicesOptions().ignoreAliases() == false,
                            randomBoolean()
                        )
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
