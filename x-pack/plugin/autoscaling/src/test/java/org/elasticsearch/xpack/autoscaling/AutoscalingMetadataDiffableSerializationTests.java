/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.autoscaling;

import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.metadata.MetadataSection;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.ChunkedToXContentDiffableSerializationTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.autoscaling.policy.AutoscalingPolicy;
import org.elasticsearch.xpack.autoscaling.policy.AutoscalingPolicyMetadata;

import java.util.SortedMap;
import java.util.TreeMap;

import static org.elasticsearch.xpack.autoscaling.AutoscalingTestCase.mutateAutoscalingPolicy;
import static org.elasticsearch.xpack.autoscaling.AutoscalingTestCase.randomAutoscalingMetadata;
import static org.elasticsearch.xpack.autoscaling.AutoscalingTestCase.randomAutoscalingPolicy;

public class AutoscalingMetadataDiffableSerializationTests extends ChunkedToXContentDiffableSerializationTestCase<MetadataSection> {

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return AutoscalingTestCase.getAutoscalingNamedWriteableRegistry();
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return AutoscalingTestCase.getAutoscalingXContentRegistry();
    }

    @Override
    protected AutoscalingMetadata doParseInstance(final XContentParser parser) {
        return AutoscalingMetadata.parse(parser);
    }

    @Override
    protected Writeable.Reader<MetadataSection> instanceReader() {
        return AutoscalingMetadata::new;
    }

    @Override
    protected AutoscalingMetadata createTestInstance() {
        return randomAutoscalingMetadata();
    }

    @Override
    protected MetadataSection makeTestChanges(final MetadataSection testInstance) {
        return mutateInstance(testInstance);
    }

    @Override
    protected MetadataSection mutateInstance(final MetadataSection instance) {
        final AutoscalingMetadata metadata = (AutoscalingMetadata) instance;
        final SortedMap<String, AutoscalingPolicyMetadata> policies = new TreeMap<>(metadata.policies());
        if (policies.size() == 0 || randomBoolean()) {
            final AutoscalingPolicy policy = randomAutoscalingPolicy();
            policies.put(policy.name(), new AutoscalingPolicyMetadata(policy));
        } else {
            // randomly remove a policy
            final String name = randomFrom(policies.keySet());
            final AutoscalingPolicyMetadata policyMetadata = policies.remove(name);
            final AutoscalingPolicy mutatedPolicy = mutateAutoscalingPolicy(policyMetadata.policy());
            policies.put(mutatedPolicy.name(), new AutoscalingPolicyMetadata(mutatedPolicy));
        }
        return new AutoscalingMetadata(policies);
    }

    @Override
    protected Writeable.Reader<Diff<MetadataSection>> diffReader() {
        return AutoscalingMetadata.AutoscalingMetadataDiff::new;
    }
}
