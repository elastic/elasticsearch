/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.template;

import org.elasticsearch.action.ingest.PutPipelineRequest;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.xcontent.XContentType;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Describes an ingest pipeline to be loaded from a resource file for use with an {@link IndexTemplateRegistry}.
 */
public class IngestPipelineConfig {
    private final String id;
    private final int version;
    private final Supplier<PutPipelineRequest> requestSupplier;

    /**
     * A list of this pipeline's dependencies, for example- such referred to through a pipeline processor.
     * This list is used to enforce proper ordering of pipeline installation, so that a pipeline gets installed only if all its
     * dependencies are already installed.
     */
    private final List<String> dependencies;

    public IngestPipelineConfig(String id, String resource, int version, String versionProperty) {
        this(id, resource, version, versionProperty, Collections.emptyList());
    }

    public IngestPipelineConfig(String id, String resource, int version, String versionProperty, List<String> dependencies) {
        this(id, version, dependencies, newResourceSupplier(id, resource, version, versionProperty));
    }

    public IngestPipelineConfig(String id, int version, List<String> dependencies, Supplier<PutPipelineRequest> requestSupplier) {
        this.id = Objects.requireNonNull(id);
        this.version = version;
        this.dependencies = dependencies;
        this.requestSupplier = Objects.requireNonNull(requestSupplier);
    }

    public String getId() {
        return id;
    }

    public int getVersion() {
        return version;
    }

    public List<String> getPipelineDependencies() {
        return dependencies;
    }

    public PutPipelineRequest getPutPipelineRequest() {
        return requestSupplier.get();
    }

    private static Supplier<PutPipelineRequest> newResourceSupplier(String id, String resource, int version, String versionProperty) {
        Objects.requireNonNull(resource);
        Objects.requireNonNull(versionProperty);
        return () -> new PutPipelineRequest(
            id,
            new BytesArray(TemplateUtils.loadTemplate(resource, String.valueOf(version), versionProperty)),
            XContentType.JSON
        );
    }
}
