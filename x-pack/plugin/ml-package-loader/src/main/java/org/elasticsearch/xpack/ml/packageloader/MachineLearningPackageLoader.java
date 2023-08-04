/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.packageloader;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.bootstrap.BootstrapCheck;
import org.elasticsearch.bootstrap.BootstrapContext;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.core.ml.packageloader.action.GetTrainedModelPackageConfigAction;
import org.elasticsearch.xpack.core.ml.packageloader.action.LoadTrainedModelPackageAction;
import org.elasticsearch.xpack.ml.packageloader.action.TransportGetTrainedModelPackageConfigAction;
import org.elasticsearch.xpack.ml.packageloader.action.TransportLoadTrainedModelPackage;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import static org.elasticsearch.core.Strings.format;

public class MachineLearningPackageLoader extends Plugin implements ActionPlugin {

    public static final String DEFAULT_ML_MODELS_REPOSITORY = "https://ml-models.elastic.co";
    public static final Setting<String> MODEL_REPOSITORY = Setting.simpleString(
        "xpack.ml.model_repository",
        DEFAULT_ML_MODELS_REPOSITORY,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    // re-using thread pool setup by the ml plugin
    public static final String UTILITY_THREAD_POOL_NAME = "ml_utility";

    private static final String MODEL_REPOSITORY_DOCUMENTATION_LINK = format(
        "https://www.elastic.co/guide/en/machine-learning/%d.%d/ml-nlp-elser.html#air-gapped-install",
        Version.CURRENT.major,
        Version.CURRENT.minor
    );

    public MachineLearningPackageLoader() {}

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

    @Override
    public List<BootstrapCheck> getBootstrapChecks() {
        return List.of(new BootstrapCheck() {
            @Override
            public BootstrapCheckResult check(BootstrapContext context) {
                try {
                    validateModelRepository(MODEL_REPOSITORY.get(context.settings()), context.environment().configFile());
                } catch (Exception e) {
                    return BootstrapCheckResult.failure(
                        "Found an invalid configuration for xpack.ml.model_repository. "
                            + e.getMessage()
                            + ". See "
                            + MODEL_REPOSITORY_DOCUMENTATION_LINK
                            + " for more information."
                    );
                }
                return BootstrapCheckResult.success();
            }

            @Override
            public boolean alwaysEnforce() {
                return true;
            }
        });
    }

    static void validateModelRepository(String repository, Path configPath) throws URISyntaxException {
        URI baseUri = new URI(repository.endsWith("/") ? repository : repository + "/").normalize();
        URI normalizedConfigUri = configPath.toUri().normalize();

        if (Strings.isNullOrEmpty(baseUri.getScheme())) {
            throw new IllegalArgumentException(
                "xpack.ml.model_repository must contain a scheme, supported schemes are \"http\", \"https\", \"file\""
            );
        }

        final String scheme = baseUri.getScheme().toLowerCase(Locale.ROOT);
        if (Set.of("http", "https", "file").contains(scheme) == false) {
            throw new IllegalArgumentException(
                "xpack.ml.model_repository must be configured with one of the following schemes: \"http\", \"https\", \"file\""
            );
        }

        if (scheme.equals("file") && (baseUri.getPath().startsWith(normalizedConfigUri.getPath()) == false)) {
            throw new IllegalArgumentException(
                "If xpack.ml.model_repository is a file location, it must be placed below the configuration: " + normalizedConfigUri
            );
        }

        if (baseUri.getUserInfo() != null) {
            throw new IllegalArgumentException("xpack.ml.model_repository does not support authentication");
        }
    }
}
