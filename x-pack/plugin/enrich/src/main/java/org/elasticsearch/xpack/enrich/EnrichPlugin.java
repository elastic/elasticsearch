/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.enrich;

import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.index.mapper.MetadataFieldMapper;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.plugins.IngestPlugin;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.Plugin;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class EnrichPlugin extends Plugin implements IngestPlugin, MapperPlugin {

    @Override
    public Map<String, MetadataFieldMapper.TypeParser> getMetadataMappers() {
        return Collections.singletonMap(EnrichSourceFieldMapper.NAME, new EnrichSourceFieldMapper.TypeParser());
    }

    @Override
    public Map<String, Processor.Factory> getProcessors(final Processor.Parameters parameters) {
        final ClusterService clusterService = parameters.ingestService.getClusterService();
        return Collections.singletonMap(EnrichProcessorFactory.TYPE,
            new EnrichProcessorFactory(clusterService::state, parameters.localShardSearcher));
    }

    @Override
    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return Collections.singletonList(new NamedWriteableRegistry.Entry(MetaData.Custom.class, EnrichMetadata.TYPE,
            EnrichMetadata::new));
    }

    public List<NamedXContentRegistry.Entry> getNamedXContent() {
        return Collections.singletonList(new NamedXContentRegistry.Entry(MetaData.Custom.class, new ParseField(EnrichMetadata.TYPE),
            EnrichMetadata::fromXContent));
    }
}
