/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.packageloader;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.core.ml.packageloader.action.GetTrainedModelPackageConfigAction;
import org.elasticsearch.xpack.core.ml.packageloader.action.LoadTrainedModelPackageAction;
import org.elasticsearch.xpack.ml.packageloader.action.TransportGetTrainedModelPackageConfigAction;
import org.elasticsearch.xpack.ml.packageloader.action.TransportLoadTrainedModelPackage;

import java.util.Arrays;
import java.util.List;

public class MachineLearningPackageLoader extends Plugin implements ActionPlugin {

    private final Settings settings;

    public static final String DEFAULT_ML_MODELS_REPOSITORY = "https://ml-models.elastic.co";
    public static final Setting<String> MODEL_REPOSITORY = Setting.simpleString(
        "xpack.ml.model_repository",
        DEFAULT_ML_MODELS_REPOSITORY,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    // re-using thread pool setup by the ml plugin
    public static final String UTILITY_THREAD_POOL_NAME = "ml_utility";

    public MachineLearningPackageLoader(Settings settings) {
        this.settings = settings;
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(MODEL_REPOSITORY);
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        // all internal, no rest endpoint
        return Arrays.asList(
            new ActionHandler<>(GetTrainedModelPackageConfigAction.INSTANCE, TransportGetTrainedModelPackageConfigAction.class),
            new ActionHandler<>(LoadTrainedModelPackageAction.INSTANCE, TransportLoadTrainedModelPackage.class)
        );
    }
}
