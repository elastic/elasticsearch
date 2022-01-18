/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;

public class FreezeStepTests extends AbstractStepTestCase<FreezeStep> {

    @Override
    public FreezeStep createRandomInstance() {
        StepKey stepKey = randomStepKey();
        StepKey nextStepKey = randomStepKey();

        return new FreezeStep(stepKey, nextStepKey, client);
    }

    @Override
    public FreezeStep mutateInstance(FreezeStep instance) {
        StepKey key = instance.getKey();
        StepKey nextKey = instance.getNextStepKey();

        switch (between(0, 1)) {
            case 0 -> key = new StepKey(key.getPhase(), key.getAction(), key.getName() + randomAlphaOfLength(5));
            case 1 -> nextKey = new StepKey(key.getPhase(), key.getAction(), key.getName() + randomAlphaOfLength(5));
            default -> throw new AssertionError("Illegal randomisation branch");
        }

        return new FreezeStep(key, nextKey, instance.getClient());
    }

    @Override
    public FreezeStep copyInstance(FreezeStep instance) {
        return new FreezeStep(instance.getKey(), instance.getNextStepKey(), instance.getClient());
    }

    private static IndexMetadata getIndexMetadata() {
        return IndexMetadata.builder(randomAlphaOfLength(10))
            .settings(settings(Version.CURRENT))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();
    }

    public void testIndexSurvives() {
        assertTrue(createRandomInstance().indexSurvives());
    }
}
