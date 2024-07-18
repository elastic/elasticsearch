/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.template;


import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.xcontent.XContentType;
import java.util.List;

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
       // return new BytesArray(loadVersionedResourceUTF8("/ingest-pipelines/" + id + ".yaml", version, variables));
        return null; // TODO
    }
}
