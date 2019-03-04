/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.dataframe.action;

import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.TransportMasterNodeReadAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.dataframe.DataFrameMessages;
import org.elasticsearch.xpack.core.dataframe.action.ValidateDataFrameTransformAction;
import org.elasticsearch.xpack.core.dataframe.action.ValidateDataFrameTransformAction.Request;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformConfig;
import org.elasticsearch.xpack.dataframe.persistence.DataFrameTransformsConfigManager;
import org.elasticsearch.xpack.dataframe.transforms.pivot.Pivot;


public class TransportValidateDataFrameTransformAction extends TransportMasterNodeReadAction<Request, AcknowledgedResponse> {

    private final XPackLicenseState licenseState;
    private final DataFrameTransformsConfigManager dataFrameTransformsConfigManager;
    private final Client client;

    @Inject
    public TransportValidateDataFrameTransformAction(TransportService transportService, ThreadPool threadPool, ActionFilters actionFilters,
                                                     IndexNameExpressionResolver indexNameExpressionResolver, ClusterService clusterService,
                                                     XPackLicenseState licenseState,
                                                     DataFrameTransformsConfigManager dataFrameTransformsConfigManager, Client client) {
        super(ValidateDataFrameTransformAction.NAME, transportService, clusterService, threadPool, actionFilters,
            indexNameExpressionResolver, Request::new);
        this.licenseState = licenseState;
        this.dataFrameTransformsConfigManager = dataFrameTransformsConfigManager;
        this.client = client;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected AcknowledgedResponse newResponse() {
        return new AcknowledgedResponse();
    }

    @Override
    protected void masterOperation(Request request, ClusterState state, ActionListener<AcknowledgedResponse> listener) throws Exception {
        if (!licenseState.isDataFrameAllowed()) {
            listener.onFailure(LicenseUtils.newComplianceException(XPackField.DATA_FRAME));
            return;
        }

        XPackPlugin.checkReadyForXPackCustomMetadata(state);

        DataFrameTransformConfig config = request.getConfig();

        // quick check whether a transform task exists with the same ID
        if (PersistentTasksCustomMetaData.getTaskWithId(state, config.getId()) != null) {
            listener.onFailure(new ResourceAlreadyExistsException(
                DataFrameMessages.getMessage(DataFrameMessages.REST_PUT_DATA_FRAME_TRANSFORM_EXISTS, config.getId())));
            return;
        }

        String[] destIndex = indexNameExpressionResolver.concreteIndexNames(state,
            IndicesOptions.lenientExpandOpen(),
            config.getDestination());

        if (destIndex.length > 0) {
            listener.onFailure(new ResourceAlreadyExistsException(
                DataFrameMessages.getMessage("Target index [{0}] already exists", config.getDestination())));
            return;
        }

        String[] srcIndices = indexNameExpressionResolver.concreteIndexNames(state,
            IndicesOptions.lenientExpandOpen(),
            config.getSource());

        if (srcIndices.length == 0) {
            listener.onFailure(new IndexNotFoundException(config.getSource()));
            return;
        }

        ActionListener<Boolean> pivotValidationListener = ActionListener.wrap(
            r -> listener.onResponse(new AcknowledgedResponse(true)),
            listener::onFailure
        );

        ActionListener<DataFrameTransformConfig> transformConfigListener = ActionListener.wrap(
            transformConfig -> listener.onFailure(new ResourceAlreadyExistsException(
                DataFrameMessages.getMessage(DataFrameMessages.REST_PUT_DATA_FRAME_TRANSFORM_EXISTS, config.getId()))),
            ex -> {
                if (ex instanceof ResourceNotFoundException) {
                    Pivot pivot = new Pivot(config.getSource(), config.getQueryConfig().getQuery(), config.getPivotConfig());
                    pivot.validate(client, pivotValidationListener);
                } else { // something failed while checking for existing config...
                    listener.onFailure(ex);
                }
            }
        );

        dataFrameTransformsConfigManager.getTransformConfiguration(config.getId(), transformConfigListener);
    }

    @Override
    protected ClusterBlockException checkBlock(Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

}
