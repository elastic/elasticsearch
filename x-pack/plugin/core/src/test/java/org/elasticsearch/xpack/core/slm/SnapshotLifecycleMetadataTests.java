/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.slm;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.test.AbstractChunkedSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ilm.OperationMode;

import java.io.IOException;
import java.util.Map;

public class SnapshotLifecycleMetadataTests extends AbstractChunkedSerializingTestCase<SnapshotLifecycleMetadata> {
    @Override
    protected SnapshotLifecycleMetadata doParseInstance(XContentParser parser) throws IOException {
        return SnapshotLifecycleMetadata.PARSER.apply(parser, null);
    }

    @Override
    protected SnapshotLifecycleMetadata createTestInstance() {
        int policyCount = randomIntBetween(0, 3);
        Map<String, SnapshotLifecyclePolicyMetadata> policies = Maps.newMapWithExpectedSize(policyCount);
        for (int i = 0; i < policyCount; i++) {
            String id = "policy-" + randomAlphaOfLength(3);
            policies.put(id, SnapshotLifecyclePolicyMetadataTests.createRandomPolicyMetadata(id));
        }
        return new SnapshotLifecycleMetadata(
            policies,
            randomFrom(OperationMode.values()),
            SnapshotLifecycleStatsTests.randomLifecycleStats()
        );
    }

    @Override
    protected SnapshotLifecycleMetadata mutateInstance(SnapshotLifecycleMetadata instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected Writeable.Reader<SnapshotLifecycleMetadata> instanceReader() {
        return SnapshotLifecycleMetadata::new;
    }
}
