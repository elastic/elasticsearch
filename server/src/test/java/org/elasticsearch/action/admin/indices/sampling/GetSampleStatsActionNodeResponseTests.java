/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.sampling;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.VersionInformation;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.ingest.SamplingService;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;

public class GetSampleStatsActionNodeResponseTests extends AbstractWireSerializingTestCase<GetSampleStatsAction.NodeResponse> {
    @Override
    protected Writeable.Reader<GetSampleStatsAction.NodeResponse> instanceReader() {
        return GetSampleStatsAction.NodeResponse::new;
    }

    @Override
    protected GetSampleStatsAction.NodeResponse createTestInstance() {
        return new GetSampleStatsAction.NodeResponse(randomNode(), randomStats());
    }

    @Override
    protected GetSampleStatsAction.NodeResponse mutateInstance(GetSampleStatsAction.NodeResponse instance) throws IOException {
        DiscoveryNode node = instance.getNode();
        SamplingService.SampleStats stats = instance.getSampleStats();
        if (randomBoolean()) {
            node = randomValueOtherThan(node, GetSampleStatsActionNodeResponseTests::randomNode);
        } else {
            stats = randomStats();
        }
        return new GetSampleStatsAction.NodeResponse(node, stats);
    }

    private static SamplingService.SampleStats randomStats() {
        return new SamplingService.SampleStats(
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomPositiveTimeValue(),
            randomPositiveTimeValue(),
            randomPositiveTimeValue(),
            randomBoolean() ? null : new ElasticsearchException("fail")
        );
    }

    private static DiscoveryNode randomNode() {
        return new DiscoveryNode(
            randomIdentifier(),
            randomIdentifier(),
            buildNewFakeTransportAddress(),
            emptyMap(),
            emptySet(),
            VersionInformation.CURRENT
        );
    }

}
