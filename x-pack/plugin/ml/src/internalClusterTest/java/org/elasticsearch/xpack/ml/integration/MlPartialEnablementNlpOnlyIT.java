/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.index.mapper.extras.MapperExtrasPlugin;
import org.elasticsearch.ingest.common.IngestCommonPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.xpack.core.ml.action.GetTrainedModelsAction;
import org.elasticsearch.xpack.core.ml.action.GetTrainedModelsStatsAction;
import org.elasticsearch.xpack.core.ml.action.MlInfoAction;
import org.elasticsearch.xpack.core.ml.action.MlMemoryAction;
import org.elasticsearch.xpack.ilm.IndexLifecycle;
import org.elasticsearch.xpack.ml.LocalStateMachineLearningNlpOnly;
import org.elasticsearch.xpack.ml.MlSingleNodeTestCase;

import java.util.Collection;

public class MlPartialEnablementNlpOnlyIT extends MlSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(
            LocalStateMachineLearningNlpOnly.class,
            DataStreamsPlugin.class,
            ReindexPlugin.class,
            IngestCommonPlugin.class,
            MockPainlessScriptEngine.TestPlugin.class,
            // ILM is required for .ml-state template index settings
            IndexLifecycle.class,
            // Needed for scaled_float
            MapperExtrasPlugin.class
        );
    }

    /**
     * The objective here is to detect if one of these very basic actions relies on some other action that is not available.
     * We don't expect them to return anything, but if they are unexpectedly calling an action that has been disabled then
     * an exception will be thrown which will fail the test.
     */
    public void testBasicInfoCalls() {
        client().execute(MlInfoAction.INSTANCE, new MlInfoAction.Request()).actionGet();
        client().execute(MlMemoryAction.INSTANCE, new MlMemoryAction.Request("*")).actionGet();
        client().execute(GetTrainedModelsAction.INSTANCE, new GetTrainedModelsAction.Request("*")).actionGet();
        client().execute(GetTrainedModelsStatsAction.INSTANCE, new GetTrainedModelsStatsAction.Request("*")).actionGet();
    }
}
