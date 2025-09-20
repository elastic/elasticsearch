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
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.test.ChunkedToXContentDiffableSerializationTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

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
            randomBoolean() ? null : ByteSizeValue.ofGb(randomIntBetween(1, 5)),
            randomBoolean() ? null : new TimeValue(randomIntBetween(1, 30), TimeUnit.DAYS),
            randomBoolean() ? new Script(ScriptType.INLINE, "painless", randomAlphaOfLength(10), Map.of()) : null
        );
    }
}
