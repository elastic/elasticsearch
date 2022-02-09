/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ilm.action;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicyMetadata;
import org.elasticsearch.xpack.ilm.LifecyclePolicyTestsUtils;

import java.util.Map;

public class TransportPutLifecycleActionTests extends ESTestCase {
    public void testIsNoop() {
        LifecyclePolicy policy1 = LifecyclePolicyTestsUtils.randomTimeseriesLifecyclePolicy("policy");
        LifecyclePolicy policy2 = randomValueOtherThan(policy1, () -> LifecyclePolicyTestsUtils.randomTimeseriesLifecyclePolicy("policy"));

        Map<String, String> headers1 = Map.of("foo", "bar");
        Map<String, String> headers2 = Map.of("foo", "eggplant");

        LifecyclePolicyMetadata existing = new LifecyclePolicyMetadata(policy1, headers1, randomNonNegativeLong(), randomNonNegativeLong());

        assertTrue(TransportPutLifecycleAction.isNoopUpdate(existing, policy1, headers1));
        assertFalse(TransportPutLifecycleAction.isNoopUpdate(existing, policy2, headers1));
        assertFalse(TransportPutLifecycleAction.isNoopUpdate(existing, policy1, headers2));
        assertFalse(TransportPutLifecycleAction.isNoopUpdate(null, policy1, headers1));
    }
}
