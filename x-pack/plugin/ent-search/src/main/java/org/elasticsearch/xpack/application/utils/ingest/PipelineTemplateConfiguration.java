/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.utils.ingest;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.ingest.PipelineConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.template.TemplateUtils;

import java.util.Objects;

public class PipelineTemplateConfiguration {

    private final String id;
    private final String resource;
    private final int version;
    private final String versionProperty;

    public PipelineTemplateConfiguration(String id, String resource, int version, String versionProperty) {
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

    public PipelineConfiguration load() {
        String config = TemplateUtils.loadTemplate(resource, String.valueOf(version), versionProperty);

        return new PipelineConfiguration(id, new BytesArray(config), XContentType.JSON);
    }
}
