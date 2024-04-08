/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml;

import org.elasticsearch.node.Node;

import java.util.Objects;

/**
 * Wrapper for the {@link MachineLearningExtension} interface that allows it to be used
 * given the way {@link Node} does Guice bindings for plugin components.
 * TODO: remove this class entirely once Guice is removed entirely.
 */
public class MachineLearningExtensionHolder {

    private final MachineLearningExtension machineLearningExtension;

    /**
     * Used by Guice, and in cases where ML is disabled.
     */
    public MachineLearningExtensionHolder() {
        this.machineLearningExtension = null;
    }

    public MachineLearningExtensionHolder(MachineLearningExtension machineLearningExtension) {
        this.machineLearningExtension = Objects.requireNonNull(machineLearningExtension);
    }

    public boolean isEmpty() {
        return machineLearningExtension == null;
    }

    public MachineLearningExtension getMachineLearningExtension() {
        return machineLearningExtension;
    }
}
