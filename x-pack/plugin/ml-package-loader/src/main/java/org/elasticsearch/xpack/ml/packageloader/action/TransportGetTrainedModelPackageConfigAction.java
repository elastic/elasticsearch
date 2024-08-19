/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.packageloader.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ModelPackageConfig;
import org.elasticsearch.xpack.core.ml.packageloader.action.GetTrainedModelPackageConfigAction;
import org.elasticsearch.xpack.core.ml.packageloader.action.GetTrainedModelPackageConfigAction.Request;
import org.elasticsearch.xpack.core.ml.packageloader.action.GetTrainedModelPackageConfigAction.Response;
import org.elasticsearch.xpack.ml.packageloader.MachineLearningPackageLoader;

import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;

import static org.elasticsearch.core.Strings.format;

public class TransportGetTrainedModelPackageConfigAction extends TransportMasterNodeAction<Request, Response> {

    private static final Logger logger = LogManager.getLogger(TransportGetTrainedModelPackageConfigAction.class);
    private final Settings settings;

    @Inject
    public TransportGetTrainedModelPackageConfigAction(
        Settings settings,
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            GetTrainedModelPackageConfigAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            GetTrainedModelPackageConfigAction.Request::new,
            indexNameExpressionResolver,
            GetTrainedModelPackageConfigAction.Response::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.settings = settings;
    }

    @Override
    protected void masterOperation(Task task, Request request, ClusterState state, ActionListener<Response> listener) throws Exception {
        String repository = MachineLearningPackageLoader.MODEL_REPOSITORY.get(settings);

        String packagedModelId = request.getPackagedModelId();
        logger.debug(() -> format("Fetch package manifest for [%s] from [%s]", packagedModelId, repository));

        threadPool.executor(MachineLearningPackageLoader.MODEL_DOWNLOAD_THREADPOOL_NAME).execute(() -> {
            try {
                URI uri = ModelLoaderUtils.resolvePackageLocation(repository, packagedModelId + ModelLoaderUtils.METADATA_FILE_EXTENSION);
                InputStream inputStream = ModelLoaderUtils.getInputStreamFromModelRepository(uri);

                try (
                    XContentParser parser = XContentType.JSON.xContent()
                        .createParser(XContentParserConfiguration.EMPTY, inputStream.readAllBytes())
                ) {
                    ModelPackageConfig packageConfig = ModelPackageConfig.fromXContentLenient(parser);

                    if (packagedModelId.equals(packageConfig.getPackagedModelId()) == false) {
                        // the package is somehow broken
                        listener.onFailure(new ElasticsearchStatusException("Invalid package name", RestStatus.INTERNAL_SERVER_ERROR));
                        return;
                    }

                    if (packageConfig.getSize() <= 0) {
                        listener.onFailure(new ElasticsearchStatusException("Invalid package size", RestStatus.INTERNAL_SERVER_ERROR));
                        return;
                    }

                    if (Strings.isNullOrEmpty(packageConfig.getSha256()) || packageConfig.getSha256().length() != 64) {
                        listener.onFailure(new ElasticsearchStatusException("Invalid package sha", RestStatus.INTERNAL_SERVER_ERROR));
                        return;
                    }

                    ModelPackageConfig withRepository = new ModelPackageConfig.Builder(packageConfig).setModelRepository(repository)
                        .build();

                    listener.onResponse(new Response(withRepository));
                }

                // TODO: use proper ElasticsearchStatusExceptions
            } catch (MalformedURLException e) {
                listener.onFailure(new IllegalArgumentException("Invalid connection configuration: " + e.getMessage(), e));
            } catch (URISyntaxException e) {
                // TODO: what if the URI contained credentials, don't leak it in the exception
                listener.onFailure(new IllegalArgumentException("Invalid connection configuration: " + e.getMessage(), e));
            } catch (ResourceNotFoundException e) {
                // TODO: don't leak the full url and package details
                listener.onFailure(new IllegalArgumentException("Failed to find package", e));
            } catch (Exception e) {
                listener.onFailure(new IllegalArgumentException("Failed to load package metadata", e));
            }
        });
    }

    @Override
    protected ClusterBlockException checkBlock(Request request, ClusterState state) {
        return null;
    }
}
