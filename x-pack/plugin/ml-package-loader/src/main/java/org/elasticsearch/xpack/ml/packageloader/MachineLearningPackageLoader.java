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

/**
 * Plugin for loading machine learning model packages in Elasticsearch.
 * <p>
 * This plugin provides functionality for downloading and importing trained ML models
 * from a configured repository (default: https://ml-models.elastic.co). It manages
 * model package downloads, including support for air-gapped installations using local
 * file repositories.
 * </p>
 * <p>
 * The plugin creates a dedicated thread pool for parallel model downloads and validates
 * the model repository configuration at bootstrap time.
 * </p>
 */
public class MachineLearningPackageLoader extends Plugin implements ActionPlugin {

    /**
     * The default URL for the Elastic ML models repository.
     */
    public static final String DEFAULT_ML_MODELS_REPOSITORY = "https://ml-models.elastic.co";

    /**
     * Setting for configuring the ML model repository location.
     * <p>
     * This can be an HTTP/HTTPS URL or a file:// URI pointing to a local directory
     * under the Elasticsearch config directory. This setting is dynamic and node-scoped.
     * </p>
     */
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

    /**
     * Name of the thread pool used for model downloads.
     */
    public static final String MODEL_DOWNLOAD_THREADPOOL_NAME = "model_download";

    /**
     * Constructs a new MachineLearningPackageLoader plugin.
     */
    public MachineLearningPackageLoader() {}

    /**
     * Returns the list of settings provided by this plugin.
     *
     * @return a list containing the model repository setting
     */
    @Override
    public List<Setting<?>> getSettings() {
        return List.of(MODEL_REPOSITORY);
    }

    /**
     * Returns the list of action handlers provided by this plugin.
     * <p>
     * These are internal actions with no REST endpoints, used for model
     * package configuration retrieval and loading operations.
     * </p>
     *
     * @return a list of action handlers for ML package operations
     */
    @Override
    public List<ActionHandler> getActions() {
        // all internal, no rest endpoint
        return Arrays.asList(
            new ActionHandler(GetTrainedModelPackageConfigAction.INSTANCE, TransportGetTrainedModelPackageConfigAction.class),
            new ActionHandler(LoadTrainedModelPackageAction.INSTANCE, TransportLoadTrainedModelPackage.class)
        );
    }

    /**
     * Returns the named writeables provided by this plugin.
     * <p>
     * Registers the model download task status for serialization across the cluster.
     * </p>
     *
     * @return a list containing the model download status entry
     */
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

    /**
     * Returns the executor builders for this plugin.
     * <p>
     * Creates a dedicated thread pool for parallel model file downloads.
     * </p>
     *
     * @param settings the node settings
     * @return a list containing the model download executor builder
     */
    @Override
    public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settings) {
        return List.of(modelDownloadExecutor(settings));
    }

    /**
     * Creates the executor builder for model downloads.
     * <p>
     * Creates a fixed-size thread pool with an unbounded queue for downloading
     * model definition files in parallel streams.
     * </p>
     *
     * @param settings the node settings
     * @return the model download executor builder
     */
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

    /**
     * Returns the bootstrap checks for this plugin.
     * <p>
     * Validates the model repository configuration at startup, ensuring it uses
     * a supported scheme (http, https, or file) and meets security requirements.
     * This check is always enforced.
     * </p>
     *
     * @return a list containing the model repository validation check
     */
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

    /**
     * Validates the model repository configuration.
     * <p>
     * Ensures the repository URI uses a supported scheme (http, https, or file),
     * does not contain authentication credentials, and if using file://, points
     * to a location under the Elasticsearch config directory.
     * </p>
     *
     * @param repository the repository URI string to validate
     * @param configPath the Elasticsearch configuration directory path
     * @throws URISyntaxException if the repository URI is malformed
     * @throws IllegalArgumentException if the repository configuration is invalid
     */
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
