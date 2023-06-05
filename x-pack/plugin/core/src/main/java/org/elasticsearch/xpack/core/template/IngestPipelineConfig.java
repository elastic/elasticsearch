/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.template;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;

import java.util.Objects;

/**
 * Describes an ingest pipeline to be loaded from a resource file for use with an {@link IndexTemplateRegistry}.
 */
public class IngestPipelineConfig {
    private final String id;
    private final String resource;
    private final int version;
    private final String versionProperty;

    public IngestPipelineConfig(String id, String resource, int version, String versionProperty) {
        this.id = Objects.requireNonNull(id);
        this.resource = Objects.requireNonNull(resource);
        this.version = version;
        this.versionProperty = Objects.requireNonNull(versionProperty);
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

    public BytesReference loadConfig() {
        return new BytesArray(TemplateUtils.loadTemplate(resource, String.valueOf(version), versionProperty));
    }
}
