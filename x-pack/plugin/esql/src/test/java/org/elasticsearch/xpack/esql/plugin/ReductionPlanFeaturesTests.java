/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.esql.plugin.ComputeService.ReductionPlanFeatures;

public class ReductionPlanFeaturesTests extends AbstractWireSerializingTestCase<ReductionPlanFeatures> {
    @Override
    protected ReductionPlanFeatures createTestInstance() {
        return randomFrom(ReductionPlanFeatures.values());
    }

    @Override
    protected ReductionPlanFeatures mutateInstance(ReductionPlanFeatures instance) {
        return ReductionPlanFeatures.values()[(instance.ordinal() + 1) % ReductionPlanFeatures.values().length];
    }

    @Override
    protected Writeable.Reader<ReductionPlanFeatures> instanceReader() {
        return ReductionPlanFeatures::read;
    }

    @Override
    protected boolean shouldBeSame(ReductionPlanFeatures newInstance) {
        return true;
    }
}
