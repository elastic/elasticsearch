/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.indexlifecycle;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.xpack.XPackPlugin;
import org.elasticsearch.xpack.XPackSettings;
import org.elasticsearch.xpack.security.InternalClient;

import java.io.IOException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

public class IndexLifecycle extends Plugin {
    private static final Logger logger = Loggers.getLogger(XPackPlugin.class);
    public static final String NAME = "index_lifecycle";
    public static final String BASE_PATH = "/_xpack/index_lifecycle/";
    public static final String THREAD_POOL_NAME = NAME;
    private final SetOnce<IndexLifecycleInitialisationService> indexLifecycleInitialisationService = new SetOnce<>();
    private Settings settings;
    private boolean enabled;
    private boolean transportClientMode;

    public static final Setting LIFECYCLE_TIMESERIES_NAME_SETTING = Setting.simpleString("index.lifecycle.name",
        Setting.Property.Dynamic, Setting.Property.IndexScope);
    public static final Setting LIFECYCLE_TIMESERIES_PHASE_SETTING = Setting.simpleString("index.lifecycle.phase",
        Setting.Property.Dynamic, Setting.Property.IndexScope);

    public IndexLifecycle(Settings settings) {
        this.settings = settings;
        this.enabled = XPackSettings.INDEX_LIFECYCLE_ENABLED.get(settings);
        this.transportClientMode = XPackPlugin.transportClientMode(settings);
    }

    public Collection<Module> nodeModules() {
        List<Module> modules = new ArrayList<>();

        if (transportClientMode) {
            return modules;
        }

        modules.add(b -> XPackPlugin.bindFeatureSet(b, IndexLifecycleFeatureSet.class));

        return modules;
    }

    @Override
    public List<Setting<?>> getSettings() {
        return Arrays.asList(LIFECYCLE_TIMESERIES_NAME_SETTING, LIFECYCLE_TIMESERIES_PHASE_SETTING);
    }

    public Collection<Object> createComponents(InternalClient internalClient, ClusterService clusterService, Clock clock) {
        indexLifecycleInitialisationService.set(new IndexLifecycleInitialisationService(settings, internalClient, clusterService, clock));
        return Collections.singletonList(indexLifecycleInitialisationService.get());
    }

    @Override
    public void close() throws IOException {
        indexLifecycleInitialisationService.get().close();
    }

}
