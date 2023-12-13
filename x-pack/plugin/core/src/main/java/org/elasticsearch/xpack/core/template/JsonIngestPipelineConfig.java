/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.template;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.xcontent.XContentType;

import java.util.List;
import java.util.Map;

public class JsonIngestPipelineConfig extends IngestPipelineConfig {
    public JsonIngestPipelineConfig(String id, String resource, int version, String versionProperty) {
        super(id, resource, version, versionProperty);
    }

    public JsonIngestPipelineConfig(String id, String resource, int version, String versionProperty, List<String> dependencies) {
        super(id, resource, version, versionProperty, dependencies);
    }

    public JsonIngestPipelineConfig(
        String id,
        String resource,
        int version,
        String versionProperty,
        List<String> dependencies,
        Map<String, String> variables
    ) {
        super(id, resource, version, versionProperty, dependencies, variables);
    }

    @Override
    public XContentType getXContentType() {
        return XContentType.JSON;
    }

    @Override
    public BytesReference loadConfig() {
        return new BytesArray(TemplateUtils.loadTemplate(resource, String.valueOf(version), versionProperty, variables));
    }
}
