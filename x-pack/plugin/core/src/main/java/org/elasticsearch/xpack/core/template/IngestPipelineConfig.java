/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.template;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Describes an ingest pipeline to be loaded from a resource file for use with an {@link IndexTemplateRegistry}.
 */
public class IngestPipelineConfig {
    private final String id;
    private final String resource;
    private final int version;
    private final String versionProperty;

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
        this.id = Objects.requireNonNull(id);
        this.resource = Objects.requireNonNull(resource);
        this.version = version;
        this.versionProperty = Objects.requireNonNull(versionProperty);
        this.dependencies = dependencies;
    }

    public String getId() {
        return id;
    }

    public int getVersion() {
        return version;
    }

    public String getVersionProperty() {
        return versionProperty;
    }

    public List<String> getPipelineDependencies() {
        return dependencies;
    }

    public BytesReference loadConfig() {
        return new BytesArray(TemplateUtils.loadTemplate(resource, String.valueOf(version), versionProperty));
    }
}
