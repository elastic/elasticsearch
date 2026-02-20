/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.sampling;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.VersionInformation;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.ingest.SamplingService;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;

public class GetSampleActionNodeResponseTests extends AbstractWireSerializingTestCase<GetSampleAction.NodeResponse> {

    @Override
    protected Writeable.Reader<GetSampleAction.NodeResponse> instanceReader() {
        return GetSampleAction.NodeResponse::new;
    }

    @Override
    protected GetSampleAction.NodeResponse createTestInstance() {
        List<SamplingService.RawDocument> sample = randomList(10, GetSampleActionNodeResponseTests::randomSample);
        return new GetSampleAction.NodeResponse(randomNode(), sample);
    }

    @Override
    protected GetSampleAction.NodeResponse mutateInstance(GetSampleAction.NodeResponse instance) throws IOException {
        DiscoveryNode node = instance.getNode();
        List<SamplingService.RawDocument> samples = new ArrayList<>(instance.getSample());
        if (randomBoolean()) {
            node = randomValueOtherThan(node, GetSampleActionNodeResponseTests::randomNode);
        } else {
            samples.add(randomSample());
        }
        return new GetSampleAction.NodeResponse(node, samples);
    }

    private static SamplingService.RawDocument randomSample() {
        return new SamplingService.RawDocument(randomIdentifier(), randomSource(), randomFrom(XContentType.values()));
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

    private static byte[] randomSource() {
        return randomByteArrayOfLength(randomIntBetween(0, 1000));
    }
}
