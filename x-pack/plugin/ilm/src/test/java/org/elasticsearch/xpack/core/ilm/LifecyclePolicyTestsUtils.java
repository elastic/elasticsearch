/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.xpack.ilm.LockableLifecycleType;

import java.util.Map;

/**
 * This class is here for constructing instances of {@link LifecyclePolicy} that differs from
 * the main {@link TimeseriesLifecycleType} one. Since the more generic constructor is package-private so
 * that users are not exposed to {@link LifecycleType}, it is still useful to construct different ones for
 * testing purposes
 */
public class LifecyclePolicyTestsUtils {

    public static LifecyclePolicy newTestLifecyclePolicy(String policyName, Map<String, Phase> phases) {
        return new LifecyclePolicy(TestLifecycleType.INSTANCE, policyName, phases);
    }

    public static LifecyclePolicy newLockableLifecyclePolicy(String policyName, Map<String, Phase> phases) {
        return new LifecyclePolicy(LockableLifecycleType.INSTANCE, policyName, phases);
    }

    public static LifecyclePolicy randomTimeseriesLifecyclePolicy(String policyName) {
        return LifecyclePolicyTests.randomTimeseriesLifecyclePolicy(policyName);
    }
}
