/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.ml.action.PutDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.utils.MlStrings;
import org.elasticsearch.xpack.ml.dataframe.analyses.DataFrameAnalysesUtils;
import org.elasticsearch.xpack.ml.dataframe.persistence.DataFrameAnalyticsConfigProvider;

import java.util.function.Supplier;

public class TransportPutDataFrameAnalyticsAction
    extends HandledTransportAction<PutDataFrameAnalyticsAction.Request, PutDataFrameAnalyticsAction.Response> {

    private final XPackLicenseState licenseState;
    private final DataFrameAnalyticsConfigProvider configProvider;

    @Inject
    public TransportPutDataFrameAnalyticsAction(TransportService transportService, ActionFilters actionFilters,
                                                XPackLicenseState licenseState, DataFrameAnalyticsConfigProvider configProvider) {
        super(PutDataFrameAnalyticsAction.NAME, transportService, actionFilters,
            (Supplier<PutDataFrameAnalyticsAction.Request>) PutDataFrameAnalyticsAction.Request::new);
        this.licenseState = licenseState;
        this.configProvider = configProvider;
    }

    @Override
    protected void doExecute(Task task, PutDataFrameAnalyticsAction.Request request,
                             ActionListener<PutDataFrameAnalyticsAction.Response> listener) {
        if (licenseState.isMachineLearningAllowed() == false) {
            listener.onFailure(LicenseUtils.newComplianceException(XPackField.MACHINE_LEARNING));
            return;
        }

        validateConfig(request.getConfig());
        configProvider.put(request.getConfig(), ActionListener.wrap(
            indexResponse -> listener.onResponse(new PutDataFrameAnalyticsAction.Response(request.getConfig())),
            listener::onFailure
        ));
    }

    private void validateConfig(DataFrameAnalyticsConfig config) {
        if (MlStrings.isValidId(config.getId()) == false) {
            throw ExceptionsHelper.badRequestException(Messages.getMessage(Messages.INVALID_ID, DataFrameAnalyticsConfig.ID,
                config.getId()));
        }
        if (!MlStrings.hasValidLengthForId(config.getId())) {
            throw ExceptionsHelper.badRequestException("id [{}] is too long; must not contain more than {} characters", config.getId(),
                MlStrings.ID_LENGTH_LIMIT);
        }
        if (config.getSource().isEmpty()) {
            throw ExceptionsHelper.badRequestException("[{}] must be non-empty", DataFrameAnalyticsConfig.SOURCE);
        }
        if (config.getDest().isEmpty()) {
            throw ExceptionsHelper.badRequestException("[{}] must be non-empty", DataFrameAnalyticsConfig.DEST);
        }
        DataFrameAnalysesUtils.readAnalyses(config.getAnalyses());
    }
}
