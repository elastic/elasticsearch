/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.common.notifications.AbstractAuditor;
import org.elasticsearch.xpack.core.ml.action.AuditMlNotificationAction;
import org.elasticsearch.xpack.ml.notifications.AnomalyDetectionAuditor;
import org.elasticsearch.xpack.ml.notifications.DataFrameAnalyticsAuditor;
import org.elasticsearch.xpack.ml.notifications.InferenceAuditor;
import org.elasticsearch.xpack.ml.notifications.SystemAuditor;

public class TransportAuditMlNotificationAction extends HandledTransportAction<AuditMlNotificationAction.Request, AcknowledgedResponse> {

    private final AnomalyDetectionAuditor anomalyDetectionAuditor;
    private final DataFrameAnalyticsAuditor dfaAuditor;
    private final InferenceAuditor inferenceAuditor;
    private final SystemAuditor systemAuditor;

    @Inject
    public TransportAuditMlNotificationAction(
        TransportService transportService,
        ActionFilters actionFilters,
        AnomalyDetectionAuditor anomalyDetectionAuditor,
        DataFrameAnalyticsAuditor dfaAuditor,
        InferenceAuditor inferenceAuditor,
        SystemAuditor systemAuditor
    ) {
        super(
            AuditMlNotificationAction.NAME,
            transportService,
            actionFilters,
            AuditMlNotificationAction.Request::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.anomalyDetectionAuditor = anomalyDetectionAuditor;
        this.dfaAuditor = dfaAuditor;
        this.inferenceAuditor = inferenceAuditor;
        this.systemAuditor = systemAuditor;
    }

    @Override
    protected void doExecute(Task task, AuditMlNotificationAction.Request request, ActionListener<AcknowledgedResponse> listener) {
        AbstractAuditor<?> auditor = switch (request.getAuditType()) {
            case ANOMALY_DETECTION -> anomalyDetectionAuditor;
            case DATAFRAME_ANALYTICS -> dfaAuditor;
            case INFERENCE -> inferenceAuditor;
            case SYSTEM -> systemAuditor;
        };

        if (auditor != null) {
            auditor.audit(request.getLevel(), request.getId(), request.getMessage());
            listener.onResponse(AcknowledgedResponse.TRUE);
        } else {
            listener.onFailure(new IllegalStateException("Cannot find audit type for auditor [" + request.getAuditType() + "]"));
        }
    }
}
