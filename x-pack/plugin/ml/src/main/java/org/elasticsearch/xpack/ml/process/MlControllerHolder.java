/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.process;

import org.elasticsearch.node.Node;

import java.util.Objects;

/**
 * Wrapper for the {@link MlController} interface that allows it to be used
 * given the way {@link Node} does Guice bindings for plugin components.
 * TODO: remove this class entirely once Guice is removed entirely.
 */
public class MlControllerHolder {

    private MlController mlController;

    public MlControllerHolder(MlController mlController) {
        this.mlController = Objects.requireNonNull(mlController);
    }

    public MlController getMlController() {
        return mlController;
    }
}
