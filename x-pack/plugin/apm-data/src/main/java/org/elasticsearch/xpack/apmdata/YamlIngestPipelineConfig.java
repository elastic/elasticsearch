/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.apmdata;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.template.IngestPipelineConfig;

import java.util.List;

import static org.elasticsearch.xpack.apmdata.ResourceUtils.loadVersionedResourceUTF8;

/**
 * An APM-plugin-specific implementation that loads ingest pipelines in yaml format from a local resources repository
 */
public class YamlIngestPipelineConfig extends IngestPipelineConfig {
    public YamlIngestPipelineConfig(String id, String resource, int version, String versionProperty, List<String> dependencies) {
        super(id, resource, version, versionProperty, dependencies);
    }

    @Override
    public XContentType getXContentType() {
        return XContentType.YAML;
    }

    @Override
    public BytesReference loadConfig() {
        return new BytesArray(loadVersionedResourceUTF8("/ingest-pipelines/" + id + ".yaml", version, variables));
    }
}
