/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference;

import org.elasticsearch.cluster.metadata.ProjectMetadata;

import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Extension point for listing inference endpoint ids registered in cluster state.
 * Implemented by the inference plugin; consumed by ML ingest model memory tracking without a compile dependency on inference.
 */
public interface InferenceEndpointRegistry {

    AtomicReference<InferenceEndpointRegistry> REFERENCE = new AtomicReference<>(Noop.INSTANCE);

    static InferenceEndpointRegistry getInstance() {
        return REFERENCE.get();
    }

    static void setInstance(InferenceEndpointRegistry instance) {
        Objects.requireNonNull(instance, "registry instance must not be null");
        REFERENCE.set(instance);
    }

    /**
     * Registered inference-endpoint ids for the project (empty when inference plugin absent).
     */
    Set<String> inferenceEndpointIds(ProjectMetadata project);

    final class Noop implements InferenceEndpointRegistry {

        static final Noop INSTANCE = new Noop();

        private Noop() {}

        @Override
        public Set<String> inferenceEndpointIds(ProjectMetadata project) {
            return Set.of();
        }
    }
}
