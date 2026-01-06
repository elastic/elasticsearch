/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.action.admin.indices.sampling;

import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ChunkedToXContentDiffableSerializationTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SamplingMetadataTests extends ChunkedToXContentDiffableSerializationTestCase<Metadata.ProjectCustom> {

    @Override
    protected SamplingMetadata doParseInstance(XContentParser parser) throws IOException {
        return SamplingMetadata.fromXContent(parser);
    }

    @Override
    protected Writeable.Reader<Metadata.ProjectCustom> instanceReader() {
        return SamplingMetadata::new;
    }

    @Override
    protected SamplingMetadata createTestInstance() {
        return new SamplingMetadata(randomSampleConfigMap());
    }

    @Override
    protected SamplingMetadata mutateInstance(Metadata.ProjectCustom instance) {
        SamplingMetadata metadata = (SamplingMetadata) instance;
        Map<String, SamplingConfiguration> map = new HashMap<>(metadata.getIndexToSamplingConfigMap());
        if (map.isEmpty() || randomBoolean()) {
            // Add a new entry
            map.put(randomAlphaOfLength(10), createRandomSampleConfig());
        } else {
            // Remove an entry
            map.remove(map.keySet().iterator().next());
        }
        return new SamplingMetadata(map);
    }

    @Override
    protected Metadata.ProjectCustom makeTestChanges(Metadata.ProjectCustom testInstance) {
        return randomValueOtherThan(testInstance, this::createTestInstance);
    }

    @Override
    protected Writeable.Reader<Diff<Metadata.ProjectCustom>> diffReader() {
        return SamplingMetadata::readDiffFrom;
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(ClusterModule.getNamedWriteables());
    }

    public void testSamplingMetadataContextExcludesSnapshot() {
        // Test that the context() method correctly excludes SNAPSHOT context
        SamplingMetadata metadata = new SamplingMetadata(Map.of());
        assertFalse("SamplingMetadata should not be restorable from snapshot", metadata.isRestorable());
        assertFalse("SamplingMetadata context should not contain SNAPSHOT", metadata.context().contains(Metadata.XContentContext.SNAPSHOT));
        assertTrue("SamplingMetadata context should contain API", metadata.context().contains(Metadata.XContentContext.API));
        assertTrue("SamplingMetadata context should contain GATEWAY", metadata.context().contains(Metadata.XContentContext.GATEWAY));
    }

    private Map<String, SamplingConfiguration> randomSampleConfigMap() {
        Map<String, SamplingConfiguration> map = new HashMap<>();
        int numConfigs = randomIntBetween(0, 5);
        for (int i = 0; i < numConfigs; i++) {
            map.put(randomAlphaOfLength(10), createRandomSampleConfig());
        }
        return map;
    }

    private static SamplingConfiguration createRandomSampleConfig() {
        return new SamplingConfiguration(
            randomDoubleBetween(0.0, 1.0, true),
            randomBoolean() ? null : randomIntBetween(1, 1000),
            randomBoolean() ? null : ByteSizeValue.ofKb(randomIntBetween(50, 100)),
            randomBoolean() ? null : new TimeValue(randomIntBetween(1, 30), TimeUnit.DAYS),
            randomBoolean() ? randomAlphaOfLength(10) : null
        );
    }
}
