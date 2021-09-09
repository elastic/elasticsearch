/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ilm;

import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicyTests;
import org.elasticsearch.xpack.core.ilm.LifecycleType;
import org.elasticsearch.xpack.core.ilm.Phase;
import org.elasticsearch.xpack.core.ilm.TestLifecycleType;
import org.elasticsearch.xpack.core.ilm.TimeseriesLifecycleType;

import java.util.Map;

import static org.elasticsearch.xpack.core.ilm.LifecyclePolicyTests.randomMeta;

/**
 * This class is here for constructing instances of {@link LifecyclePolicy} that differs from
 * the main {@link TimeseriesLifecycleType} one. Since the more generic constructor is package-private so
 * that users are not exposed to {@link LifecycleType}, it is still useful to construct different ones for
 * testing purposes
 */
public class LifecyclePolicyTestsUtils {

    public static LifecyclePolicy newTestLifecyclePolicy(String policyName, Map<String, Phase> phases) {
        return new LifecyclePolicy(TestLifecycleType.INSTANCE, policyName, phases, randomMeta());
    }

    public static LifecyclePolicy newLockableLifecyclePolicy(String policyName, Map<String, Phase> phases) {
        return new LifecyclePolicy(LockableLifecycleType.INSTANCE, policyName, phases, randomMeta());
    }

    public static LifecyclePolicy randomTimeseriesLifecyclePolicy(String policyName) {
        return LifecyclePolicyTests.randomTimeseriesLifecyclePolicy(policyName);
    }
}
