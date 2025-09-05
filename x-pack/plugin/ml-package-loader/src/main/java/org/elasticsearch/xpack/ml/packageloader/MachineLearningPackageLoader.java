/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.packageloader;

import org.elasticsearch.Build;
import org.elasticsearch.bootstrap.BootstrapCheck;
import org.elasticsearch.bootstrap.BootstrapContext;
import org.elasticsearch.common.ReferenceDocs;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.xpack.core.ml.packageloader.action.GetTrainedModelPackageConfigAction;
import org.elasticsearch.xpack.core.ml.packageloader.action.LoadTrainedModelPackageAction;
import org.elasticsearch.xpack.ml.packageloader.action.ModelDownloadTask;
import org.elasticsearch.xpack.ml.packageloader.action.ModelImporter;
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

    // This link will be invalid for serverless, but serverless will never be
    // air-gapped, so this message should never be needed.
    private static final String MODEL_REPOSITORY_DOCUMENTATION_LINK = format(
        "https://www.elastic.co/guide/en/machine-learning/%s/ml-nlp-elser.html#air-gapped-install",
        Build.current().version().replaceFirst("^(\\d+\\.\\d+).*", "$1")
    );

    public static final String MODEL_DOWNLOAD_THREADPOOL_NAME = "model_download";

    public MachineLearningPackageLoader() {}

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(MODEL_REPOSITORY);
    }

    @Override
    public List<ActionHandler> getActions() {
        // all internal, no rest endpoint
        return Arrays.asList(
            new ActionHandler(GetTrainedModelPackageConfigAction.INSTANCE, TransportGetTrainedModelPackageConfigAction.class),
            new ActionHandler(LoadTrainedModelPackageAction.INSTANCE, TransportLoadTrainedModelPackage.class)
        );
    }

    @Override
    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return List.of(
            new NamedWriteableRegistry.Entry(
                Task.Status.class,
                ModelDownloadTask.DownloadStatus.NAME,
                ModelDownloadTask.DownloadStatus::new
            )
        );
    }

    @Override
    public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settings) {
        return List.of(modelDownloadExecutor(settings));
    }

    public static FixedExecutorBuilder modelDownloadExecutor(Settings settings) {
        // Threadpool with a fixed number of threads for
        // downloading the model definition files
        return new FixedExecutorBuilder(
            settings,
            MODEL_DOWNLOAD_THREADPOOL_NAME,
            ModelImporter.NUMBER_OF_STREAMS,
            -1, // unbounded queue size
            "xpack.ml.model_download_thread_pool",
            EsExecutors.TaskTrackingConfig.DO_NOT_TRACK
        );
    }

    @Override
    public List<BootstrapCheck> getBootstrapChecks() {
        return List.of(new BootstrapCheck() {
            @Override
            public BootstrapCheckResult check(BootstrapContext context) {
                try {
                    validateModelRepository(MODEL_REPOSITORY.get(context.settings()), context.environment().configDir());
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

            @Override
            public ReferenceDocs referenceDocs() {
                return ReferenceDocs.BOOTSTRAP_CHECKS;
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
