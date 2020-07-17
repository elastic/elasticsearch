/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.autoscaling.policy;

import org.elasticsearch.cluster.AbstractDiffable;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractDiffableSerializationTestCase;
import org.elasticsearch.xpack.autoscaling.AutoscalingTestCase;

import static org.elasticsearch.xpack.autoscaling.AutoscalingTestCase.mutateAutoscalingPolicy;
import static org.elasticsearch.xpack.autoscaling.AutoscalingTestCase.randomAutoscalingPolicyOfName;

public class AutoscalingPolicyMetadataDiffableSerializationTests extends AbstractDiffableSerializationTestCase<AutoscalingPolicyMetadata> {

    private final String name = randomAlphaOfLength(8);

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return AutoscalingTestCase.getAutoscalingNamedWriteableRegistry();
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return AutoscalingTestCase.getAutoscalingXContentRegistry();
    }

    @Override
    protected AutoscalingPolicyMetadata doParseInstance(final XContentParser parser) {
        return AutoscalingPolicyMetadata.parse(parser, name);
    }

    @Override
    protected Writeable.Reader<AutoscalingPolicyMetadata> instanceReader() {
        return AutoscalingPolicyMetadata::new;
    }

    @Override
    protected AutoscalingPolicyMetadata createTestInstance() {
        return new AutoscalingPolicyMetadata(randomAutoscalingPolicyOfName(name));
    }

    @Override
    protected AutoscalingPolicyMetadata makeTestChanges(final AutoscalingPolicyMetadata testInstance) {
        return mutateInstance(testInstance);
    }

    @Override
    protected AutoscalingPolicyMetadata mutateInstance(final AutoscalingPolicyMetadata instance) {
        return new AutoscalingPolicyMetadata(mutateAutoscalingPolicy(instance.policy()));
    }

    @Override
    protected Writeable.Reader<Diff<AutoscalingPolicyMetadata>> diffReader() {
        return in -> AbstractDiffable.readDiffFrom(AutoscalingPolicyMetadata::new, in);
    }

}
