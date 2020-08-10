/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.plugin.datasketches;

import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.aggregations.metrics.datasketches.hll.HllSketchAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.datasketches.hll.InternalHllSketchAggregation;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.function.Supplier;

/**
 * A plugin to support datasketes aggregation.
 */
public class DataSketchesPlugin extends Plugin implements SearchPlugin {

    public DataSketchesPlugin(final Settings settings) {}

    @Override
    public Collection<Object> createComponents(
        Client client,
        ClusterService clusterService,
        ThreadPool threadPool,
        ResourceWatcherService resourceWatcherService,
        ScriptService scriptService,
        NamedXContentRegistry xContentRegistry,
        Environment environment,
        NodeEnvironment nodeEnvironment,
        NamedWriteableRegistry namedWriteableRegistry,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<RepositoriesService> repositoriesServiceSupplier
    ) {

        return super.createComponents(
            client,
            clusterService,
            threadPool,
            resourceWatcherService,
            scriptService,
            xContentRegistry,
            environment,
            nodeEnvironment,
            namedWriteableRegistry,
            indexNameExpressionResolver,
            repositoriesServiceSupplier
        );
    }

    @Override
    public ArrayList<AggregationSpec> getAggregations() {
        ArrayList<SearchPlugin.AggregationSpec> r = new ArrayList<>();
        r.add(
            new AggregationSpec(HllSketchAggregationBuilder.NAME, HllSketchAggregationBuilder::new, HllSketchAggregationBuilder.PARSER)
                .addResultReader(InternalHllSketchAggregation::new)
                .setAggregatorRegistrar(HllSketchAggregationBuilder::registerAggregators)
        );

        return r;
    }
}
