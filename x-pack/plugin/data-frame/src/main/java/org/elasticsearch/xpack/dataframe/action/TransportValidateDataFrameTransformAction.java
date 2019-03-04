/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.dataframe.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.TransportMasterNodeReadAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.dataframe.DataFrameMessages;
import org.elasticsearch.xpack.core.dataframe.action.ValidateDataFrameTransformAction;
import org.elasticsearch.xpack.core.dataframe.action.ValidateDataFrameTransformAction.Request;
import org.elasticsearch.xpack.core.dataframe.action.ValidateDataFrameTransformAction.Response;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformConfig;
import org.elasticsearch.xpack.dataframe.persistence.DataFrameTransformsConfigManager;
import org.elasticsearch.xpack.dataframe.transforms.pivot.Pivot;
import org.elasticsearch.xpack.dataframe.transforms.pivot.PivotException;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.core.dataframe.DataFrameMessages.REST_VALIDATE_VALIDATION_ERROR_MSG;


public class TransportValidateDataFrameTransformAction extends TransportMasterNodeReadAction<Request, Response> {

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
    protected Response newResponse() {
        return new Response();
    }

    @Override
    protected void masterOperation(Request request, ClusterState state, ActionListener<Response> listener) throws Exception {
        if (!licenseState.isDataFrameAllowed()) {
            listener.onFailure(LicenseUtils.newComplianceException(XPackField.DATA_FRAME));
            return;
        }

        Set<String> errors = new LinkedHashSet<>();
        Set<String> warnings = new LinkedHashSet<>();

        XPackPlugin.checkReadyForXPackCustomMetadata(state);

        DataFrameTransformConfig config = request.getConfig();

        // quick check whether a transform task exists with the same ID
        if (PersistentTasksCustomMetaData.getTaskWithId(state, config.getId()) != null) {
            errors.add(DataFrameMessages.getMessage(DataFrameMessages.REST_PUT_DATA_FRAME_TRANSFORM_EXISTS, config.getId()));
        }

        String[] destIndex = indexNameExpressionResolver.concreteIndexNames(state,
            IndicesOptions.lenientExpandOpen(),
            config.getDestination());

        if (destIndex.length > 0) {
            errors.add(DataFrameMessages.getMessage(DataFrameMessages.REST_VALIDATE_DATA_FRAME_DEST_INDEX_EXISTS, config.getDestination()));
        }

        String[] srcIndices = indexNameExpressionResolver.concreteIndexNames(state,
            IndicesOptions.lenientExpandOpen(),
            config.getSource());

        if (srcIndices.length == 0) {
            errors.add(DataFrameMessages.getMessage(DataFrameMessages.REST_VALIDATE_DATA_FRAME_MISSING_SOURCE_INDEX, config.getSource()));
            listener.onFailure(ValidationException.fromErrorsAndWarnings(errors, warnings));
            return;
        }
        final Pivot pivot = new Pivot(config.getSource(), config.getQueryConfig().getQuery(), config.getPivotConfig());

        ActionListener<List<Map<String, Object>>> pivotValidationListener = ActionListener.wrap(
            r -> {
                if (r.isEmpty()) {
                    warnings.add("Configuration returned empty results from source index [" + config.getSource() + "]");
                }
                if (errors.isEmpty()) {
                    listener.onResponse(new Response(warnings));
                } else {
                    listener.onFailure(ValidationException.fromErrorsAndWarnings(errors, warnings));
                }
            },
            ex -> {
                if (ex instanceof PivotException) {
                    errors.add(ex.getMessage());
                    listener.onFailure(ValidationException.fromErrorsAndWarnings(errors, warnings));
                } else {
                    listener.onFailure(ex);
                }
            }
        );

        ActionListener<DataFrameTransformConfig> transformConfigListener = ActionListener.wrap(
            transformConfig -> {
                pivot.validate(client, pivotValidationListener);
                listener.onFailure(ValidationException.fromErrorsAndWarnings(errors, warnings));
            },
            ex -> {
                if (ex instanceof ResourceNotFoundException) {
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

    public static class ValidationException extends ElasticsearchException {

        static ValidationException fromErrorsAndWarnings(Set<String> errors, Set<String> warnings) {
            String msg = DataFrameMessages.getMessage(REST_VALIDATE_VALIDATION_ERROR_MSG,
                Strings.collectionToDelimitedString(errors, ", "),
                Strings.collectionToDelimitedString(warnings, ", "));
            return new ValidationException(msg);
        }

        ValidationException(String msg) {
            super(msg);
        }

        @Override
        public final RestStatus status() {
            return RestStatus.BAD_REQUEST;
        }
    }

}
