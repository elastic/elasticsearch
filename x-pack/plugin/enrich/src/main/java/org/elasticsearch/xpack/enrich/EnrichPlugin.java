/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.enrich;

import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.plugins.IngestPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class EnrichPlugin extends Plugin implements IngestPlugin {

    @Override
    public Map<String, Processor.Factory> getProcessors(Processor.Parameters parameters) {
        return Collections.emptyMap();
    }

    @Override
    public Collection<Object> createComponents(Client client,
                                               ClusterService clusterService,
                                               ThreadPool threadPool,
                                               ResourceWatcherService resourceWatcherService,
                                               ScriptService scriptService,
                                               NamedXContentRegistry xContentRegistry,
                                               Environment environment,
                                               NodeEnvironment nodeEnvironment,
                                               NamedWriteableRegistry namedWriteableRegistry) {
        EnrichStore enrichStore = new EnrichStore(clusterService);
        EnrichPolicyRunner enrichPolicyRunner = new EnrichPolicyRunner(clusterService, client, enrichStore,
            new IndexNameExpressionResolver(), System::currentTimeMillis);
        return Arrays.asList(enrichStore, enrichPolicyRunner);
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
