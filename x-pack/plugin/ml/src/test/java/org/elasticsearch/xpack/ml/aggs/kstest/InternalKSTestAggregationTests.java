/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.kstest;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.test.InternalAggregationTestCase;
import org.elasticsearch.xpack.ml.MachineLearningTests;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class InternalKSTestAggregationTests extends InternalAggregationTestCase<InternalKSTestAggregation> {

    @Override
    protected SearchPlugin registerPlugin() {
        return MachineLearningTests.createTrialLicensedMachineLearning(Settings.EMPTY);
    }

    @Override
    protected InternalKSTestAggregation createTestInstance(String name, Map<String, Object> metadata) {
        List<String> modes = randomSubsetOf(Arrays.stream(Alternative.values()).map(Alternative::toString).collect(Collectors.toList()));
        return new InternalKSTestAggregation(
            name,
            metadata,
            modes.stream().collect(Collectors.toMap(Function.identity(), a -> randomDouble()))
        );
    }

    @Override
    protected void assertReduced(InternalKSTestAggregation reduced, List<InternalKSTestAggregation> inputs) {
        // no test since reduce operation is unsupported
    }

    @Override
    public void testReduceRandom() {
        expectThrows(UnsupportedOperationException.class, () -> createTestInstance("name", null).getReducer(null, 0));
    }

    @Override
    protected InternalKSTestAggregation mutateInstance(InternalKSTestAggregation instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }
}
