/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.searchablesnapshots.action;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class MountSearchableSnapshotRequestTests extends AbstractWireSerializingTestCase<MountSearchableSnapshotRequest> {
    public void testSource() throws Exception {
        final MountSearchableSnapshotRequest original = randomState(createTestInstance());
        XContentBuilder builder = original.toXContent(XContentFactory.jsonBuilder(), new ToXContent.MapParams(Collections.emptyMap()));
        XContentParser parser = XContentType.JSON.xContent().createParser(
            NamedXContentRegistry.EMPTY, null, BytesReference.bytes(builder).streamInput());
        Map<String, Object> map = parser.mapOrdered();

        // we will only restore properties from the map that are contained in the request body. All other
        // properties are restored from the original (in the actual REST action this is restored from the
        // REST path and request parameters).
        final MountSearchableSnapshotRequest processed = new MountSearchableSnapshotRequest(original.mountedIndex());
        processed.masterNodeTimeout(original.masterNodeTimeout());
        processed.waitForCompletion(original.waitForCompletion());

        processed.source(map);

        assertEquals(original, processed);
    }

    private MountSearchableSnapshotRequest randomState(MountSearchableSnapshotRequest instance) {
        if (randomBoolean()) {
            instance.snapshotIndex(randomAlphaOfLength(randomInt(3) + 2));
        }
        if (randomBoolean()) {
            Map<String, Object> settings = new HashMap<>();
            int count = randomInt(3) + 1;
            for (int i = 0; i < count; ++i) {
                settings.put(randomAlphaOfLengthBetween(2, 5), randomAlphaOfLengthBetween(2, 5));
            }
            instance.settings(settings);
        }
        if (randomBoolean()) {
            Map<String, Object> indexSettings = new HashMap<>();
            int count = randomInt(3) + 1;
            for (int i = 0; i < count; ++i) {
                indexSettings.put(randomAlphaOfLengthBetween(2, 5), randomAlphaOfLengthBetween(2, 5));
            }
            instance.indexSettings(indexSettings);
        }

        instance.waitForCompletion(randomBoolean());

        if (randomBoolean()) {
            instance.masterNodeTimeout(randomTimeValue());
        }
        return instance;
    }

    @Override
    protected MountSearchableSnapshotRequest createTestInstance() {
        MountSearchableSnapshotRequest req = new MountSearchableSnapshotRequest(randomAlphaOfLength(5));
        req.snapshot(randomAlphaOfLength(4));
        req.snapshotIndex(randomAlphaOfLength(5));
        req.repository(randomAlphaOfLength(6));
        return randomState(req);
    }

    @Override
    protected Writeable.Reader<MountSearchableSnapshotRequest> instanceReader() {
        return MountSearchableSnapshotRequest::new;
    }

    @Override
    protected MountSearchableSnapshotRequest mutateInstance(MountSearchableSnapshotRequest instance) throws IOException {
        MountSearchableSnapshotRequest copy = copyInstance(instance);
        // ensure that at least one property is different
        copy.repository("copied-" + instance.repository());
        return randomState(copy);
    }
}
