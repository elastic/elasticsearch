/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xpack.core.inference.InferenceRequestStats;
import org.elasticsearch.xpack.core.inference.InferenceRequestStatsTests;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;

import java.io.IOException;
import java.util.HashMap;

public class GetInternalInferenceUsageActionNodeResponseTests extends AbstractBWCWireSerializationTestCase<
    GetInternalInferenceUsageAction.NodeResponse> {
    public static GetInternalInferenceUsageAction.NodeResponse createRandom() {
        DiscoveryNode node = DiscoveryNodeUtils.create("id");
        var stats = new HashMap<String, InferenceRequestStats>();

        for (int i = 0; i < randomIntBetween(1, 10); i++) {
            stats.put(randomAlphaOfLength(10), InferenceRequestStatsTests.createRandom());
        }

        return new GetInternalInferenceUsageAction.NodeResponse(node, stats);
    }

    @Override
    protected Writeable.Reader<GetInternalInferenceUsageAction.NodeResponse> instanceReader() {
        return GetInternalInferenceUsageAction.NodeResponse::new;
    }

    @Override
    protected GetInternalInferenceUsageAction.NodeResponse createTestInstance() {
        return createRandom();
    }

    @Override
    protected GetInternalInferenceUsageAction.NodeResponse mutateInstance(GetInternalInferenceUsageAction.NodeResponse instance)
        throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    @Override
    protected GetInternalInferenceUsageAction.NodeResponse mutateInstanceForVersion(
        GetInternalInferenceUsageAction.NodeResponse instance,
        TransportVersion version
    ) {
        return instance;
    }
}
