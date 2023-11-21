/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform;

import org.elasticsearch.node.Node;

import java.util.Objects;

/**
 * Wrapper for the {@link TransformExtension} interface that allows it to be used
 * given the way {@link Node} does Guice bindings for plugin components.
 * TODO: remove this class entirely once Guice is removed entirely.
 */
public class TransformExtensionHolder {

    private final TransformExtension transformExtension;

    /**
     * Used by Guice.
     */
    public TransformExtensionHolder() {
        this.transformExtension = null;
    }

    public TransformExtensionHolder(TransformExtension transformExtension) {
        this.transformExtension = Objects.requireNonNull(transformExtension);
    }

    public boolean isEmpty() {
        return transformExtension == null;
    }

    public TransformExtension getTransformExtension() {
        return transformExtension;
    }
}
