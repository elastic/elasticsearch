/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.dataframe.DataFrameMessages;
import org.elasticsearch.xpack.dataframe.action.PutDataFrameTransformAction.Request;
import org.elasticsearch.xpack.dataframe.action.PutDataFrameTransformAction.Response;
import org.elasticsearch.xpack.dataframe.persistence.DataFrameTransformsConfigManager;
import org.elasticsearch.xpack.dataframe.persistence.DataframeIndex;
import org.elasticsearch.xpack.dataframe.transforms.DataFrameTransform;
import org.elasticsearch.xpack.dataframe.transforms.pivot.Validator;

public class TransportPutDataFrameTransformAction
        extends TransportMasterNodeAction<PutDataFrameTransformAction.Request, PutDataFrameTransformAction.Response> {

    private static final Logger logger = LogManager.getLogger(TransportPutDataFrameTransformAction.class);

    private final XPackLicenseState licenseState;
    private final PersistentTasksService persistentTasksService;
    private final Client client;
    private final DataFrameTransformsConfigManager dataFrameTransformsConfigManager;

    @Inject
    public TransportPutDataFrameTransformAction(TransportService transportService, ThreadPool threadPool, ActionFilters actionFilters,
            IndexNameExpressionResolver indexNameExpressionResolver, ClusterService clusterService, XPackLicenseState licenseState,
            PersistentTasksService persistentTasksService, DataFrameTransformsConfigManager dataFrameTransformsConfigManager,
            Client client) {
        super(PutDataFrameTransformAction.NAME, transportService, clusterService, threadPool, actionFilters, indexNameExpressionResolver,
                PutDataFrameTransformAction.Request::new);
        this.licenseState = licenseState;
        this.persistentTasksService = persistentTasksService;
        this.client = client;
        this.dataFrameTransformsConfigManager = dataFrameTransformsConfigManager;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected PutDataFrameTransformAction.Response newResponse() {
        return new PutDataFrameTransformAction.Response();
    }

    @Override
    protected void masterOperation(Request request, ClusterState clusterState, ActionListener<Response> listener) throws Exception {

        if (!licenseState.isDataFrameAllowed()) {
            listener.onFailure(LicenseUtils.newComplianceException(XPackField.DATA_FRAME));
            return;
        }

        XPackPlugin.checkReadyForXPackCustomMetadata(clusterState);

        String transformId = request.getConfig().getId();
        // quick check whether a transform has already been created under that name
        if (PersistentTasksCustomMetaData.getTaskWithId(clusterState, transformId) != null) {
            listener.onFailure(new ResourceAlreadyExistsException(
                    DataFrameMessages.getMessage(DataFrameMessages.REST_PUT_DATA_FRAME_TRANSFORM_EXISTS, transformId)));
            return;
        }

        // create the transform, note the non-state creating steps are done first, so we minimize the chance to end up with orphaned state
        // transform validation
        Validator transformValidator = new Validator(request.getConfig(), client);
        transformValidator.validate(ActionListener.wrap(validationResult -> {
            // deduce target mappings
            transformValidator.deduceMappings(ActionListener.wrap(mappings -> {
                // create the destination index
                DataframeIndex.createDestinationIndex(client, request.getConfig(), mappings, ActionListener.wrap(createIndexResult -> {
                    DataFrameTransform transform = createDataFrameTransform(transformId, threadPool);
                    // create the transform configuration and store it in the internal index
                    dataFrameTransformsConfigManager.putTransformConfiguration(request.getConfig(), ActionListener.wrap(r -> {
                        // finally start the persistent task
                        persistentTasksService.sendStartRequest(transform.getId(), DataFrameTransform.NAME, transform,
                                ActionListener.wrap(persistentTask -> {
                                    listener.onResponse(new PutDataFrameTransformAction.Response(true));
                        }, startPersistentTaskException -> {
                            // delete the otherwise orphaned transform configuration, for now we do not delete the destination index
                            dataFrameTransformsConfigManager.deleteTransformConfiguration(transformId, ActionListener.wrap(r2 -> {
                                        logger.debug("Deleted data frame transform [{}] configuration from data frame configuration index",
                                                transformId);
                                        listener.onFailure(
                                        new RuntimeException(
                                                DataFrameMessages.getMessage(
                                                        DataFrameMessages.REST_PUT_DATA_FRAME_FAILED_TO_START_PERSISTENT_TASK, r2),
                                                startPersistentTaskException));
                            }, deleteTransformFromIndexException -> {
                                logger.error("Failed to cleanup orphaned data frame transform [{}] configuration", transformId);
                                listener.onFailure(
                                        new RuntimeException(
                                                DataFrameMessages.getMessage(
                                                        DataFrameMessages.REST_PUT_DATA_FRAME_FAILED_TO_START_PERSISTENT_TASK, false),
                                                startPersistentTaskException));
                            }));
                        }));
                    }, listener::onFailure));
                }, createDestinationIndexException -> {
                    listener.onFailure(new RuntimeException(DataFrameMessages.REST_PUT_DATA_FRAME_FAILED_TO_CREATE_TARGET_INDEX,
                            createDestinationIndexException));
                }));
            }, deduceTargetMappingsException -> {
                listener.onFailure(new RuntimeException(DataFrameMessages.REST_PUT_DATA_FRAME_FAILED_TO_DEDUCE_TARGET_MAPPINGS,
                        deduceTargetMappingsException));
            }));
        }, validationException -> {
            listener.onFailure(new RuntimeException(DataFrameMessages.REST_PUT_DATA_FRAME_FAILED_TO_VALIDATE_DATA_FRAME_CONFIGURATION,
                    validationException));
        }));
    }

    private static DataFrameTransform createDataFrameTransform(String transformId, ThreadPool threadPool) {
        return new DataFrameTransform(transformId);
    }

    @Override
    protected ClusterBlockException checkBlock(PutDataFrameTransformAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
