/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.deployment;

import org.elasticsearch.test.ESTestCase;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class MlPlatformArchitecturesUtilTests extends ESTestCase {

    public void setUp() throws Exception {
        super.setUp();
    }

    public void testGetNodesOsArchitectures() {
        // cannot test due to static dependencies (executeAsyncWithOrigin)
    }

    public void testVerifyArchitectureOfMLNodesIsHomogenous_GivenZeroArches() {
        var architectures = new HashSet<String>();
        MLPlatformArchitecturesUtil.verifyArchitectureOfMLNodesIsHomogenous(
            architectures,
            randomAlphaOfLength(10),
            randomAlphaOfLength(10)
        );
    }

    public void testVerifyArchitectureOfMLNodesIsHomogenous_GivenOneArch() {
        var architectures = nArchitectures(1);
        MLPlatformArchitecturesUtil.verifyArchitectureOfMLNodesIsHomogenous(
            architectures,
            randomAlphaOfLength(10),
            randomAlphaOfLength(10)
        );
    }

    public void testThrowsVerifyArchitectureOfMLNodesIsHomogenous_GivenAtLeastTwoArches() {
        var architectures = nArchitectures(randomIntBetween(2, 10));
        String architecturesStr = architectures.toString();
        architecturesStr = architecturesStr.substring(1, architecturesStr.length() - 1); // Remove the brackets
        var modelId = randomAlphaOfLength(10);
        var requiredArch = randomAlphaOfLength(10);
        String message = "ML nodes in this cluster have multiple platform architectures, "
            + "but can only have one for this model (["
            + modelId
            + "]); "
            + "expected ["
            + requiredArch
            + "]; but was ["
            + architecturesStr
            + "]";

        Throwable exception = expectThrows(
            IllegalStateException.class,
            "Expected IllegalStateException but no exception was thrown",
            () -> MLPlatformArchitecturesUtil.verifyArchitectureOfMLNodesIsHomogenous(architectures, requiredArch, modelId)
        );
        assertEquals(exception.getMessage(), message);
    }

    public Set<String> nArchitectures(Integer n) {
        Set<String> architectures = new HashSet<String>();
        for (int i = 0; i < n; i++) {
            architectures.add(randomAlphaOfLength(10));
        }
        return architectures;
    }

    public void testVerifyArchitectureMatchesModelPlatformArchitecture_GivenRequiredArchMatches() {
        var requiredArch = randomAlphaOfLength(10);
        String architecturesStr = requiredArch;

        var modelId = randomAlphaOfLength(10);
        String message = "The model being deployed (["
            + modelId
            + "]) is platform specific and incompatible with ML nodes in the cluster; "
            + "expected ["
            + requiredArch
            + "]; but was ["
            + architecturesStr
            + "]";

        MLPlatformArchitecturesUtil.verifyArchitectureMatchesModelPlatformArchitecture(
            new HashSet<>(Collections.singleton(architecturesStr)),
            requiredArch,
            modelId
        );
    }

    public void testVerifyArchitectureMatchesModelPlatformArchitecture_GivenRequiredArchDoesNotMatch() {
        var requiredArch = randomAlphaOfLength(10);
        String architecturesStr = requiredArch + "-DIFFERENT";

        var modelId = randomAlphaOfLength(10);
        String message = "The model being deployed (["
            + modelId
            + "]) is platform specific and incompatible with ML nodes in the cluster; "
            + "expected ["
            + requiredArch
            + "]; but was ["
            + architecturesStr
            + "]";

        Throwable exception = expectThrows(
            IllegalArgumentException.class,
            "Expected IllegalArgumentException but no exception was thrown",
            () -> MLPlatformArchitecturesUtil.verifyArchitectureMatchesModelPlatformArchitecture(
                new HashSet<>(Collections.singleton(architecturesStr)),
                requiredArch,
                modelId
            )
        );
        assertEquals(exception.getMessage(), message);
    }

    public void testVerifyArchitectureMatchesModelPlatformArchitecture_GivenTooManyArches() {
        var requiredArch = randomAlphaOfLength(10);
        var architectures = nArchitectures(randomIntBetween(2, 10));
        String architecturesStr = architectures.toString();
        architecturesStr = architecturesStr.substring(1, architecturesStr.length() - 1); // Remove the brackets

        var modelId = randomAlphaOfLength(10);
        String message = "The model being deployed (["
            + modelId
            + "]) is platform specific and incompatible with ML nodes in the cluster; "
            + "expected ["
            + requiredArch
            + "]; but was ["
            + architecturesStr
            + "]";

        Throwable exception = expectThrows(
            IllegalArgumentException.class,
            "Expected IllegalArgumentException but no exception was thrown",
            () -> MLPlatformArchitecturesUtil.verifyArchitectureMatchesModelPlatformArchitecture(architectures, requiredArch, modelId)
        );
        assertEquals(exception.getMessage(), message);
    }
}
