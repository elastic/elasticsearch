/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.rest.generative;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.xpack.esql.generator.GenerativeFeature;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * {@link GenerativeRestTest} parameterized once per {@link GenerativeFeature}, plus a {@code null} baseline.
 * Each case can be muted independently in CI.
 */
public abstract class PerFeatureGenerativeRestTest extends GenerativeRestTest {

    @ParametersFactory(argumentFormatting = "feature:%1$s")
    public static List<Object[]> parameters() {
        List<Object[]> args = new ArrayList<>();
        args.add(new Object[] { null });
        for (GenerativeFeature feature : GenerativeFeature.values()) {
            args.add(new Object[] { feature });
        }
        return args;
    }

    private final Set<GenerativeFeature> features;

    protected PerFeatureGenerativeRestTest(GenerativeFeature feature) {
        this.features = feature == null ? Set.of() : Set.of(feature);
    }

    @Override
    protected Set<GenerativeFeature> enabledFeatures() {
        return features;
    }
}
