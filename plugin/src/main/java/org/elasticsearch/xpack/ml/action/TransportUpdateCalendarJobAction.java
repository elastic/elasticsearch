/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.ml.MLMetadataField;
import org.elasticsearch.xpack.ml.MlMetadata;
import org.elasticsearch.xpack.ml.job.persistence.JobProvider;
import org.elasticsearch.xpack.ml.utils.ExceptionsHelper;

public class TransportUpdateCalendarJobAction extends HandledTransportAction<UpdateCalendarJobAction.Request, PutCalendarAction.Response> {

    private final ClusterService clusterService;
    private final JobProvider jobProvider;

    @Inject
    public TransportUpdateCalendarJobAction(Settings settings, ThreadPool threadPool,
                                            TransportService transportService, ActionFilters actionFilters,
                                            IndexNameExpressionResolver indexNameExpressionResolver,
                                            ClusterService clusterService, JobProvider jobProvider) {
        super(settings, UpdateCalendarJobAction.NAME, threadPool, transportService, actionFilters,
                indexNameExpressionResolver, UpdateCalendarJobAction.Request::new);
        this.clusterService = clusterService;
        this.jobProvider = jobProvider;
    }

    @Override
    protected void doExecute(UpdateCalendarJobAction.Request request, ActionListener<PutCalendarAction.Response> listener) {
        ClusterState state = clusterService.state();
        MlMetadata mlMetadata = state.getMetaData().custom(MLMetadataField.TYPE);
        for (String jobToAdd: request.getJobIdsToAdd()) {
            if (mlMetadata.isGroupOrJob(jobToAdd) == false) {
                listener.onFailure(ExceptionsHelper.missingJobException(jobToAdd));
                return;
            }
        }

        for (String jobToRemove: request.getJobIdsToRemove()) {
            if (mlMetadata.isGroupOrJob(jobToRemove) == false) {
                listener.onFailure(ExceptionsHelper.missingJobException(jobToRemove));
                return;
            }
        }

        jobProvider.updateCalendar(request.getCalendarId(), request.getJobIdsToAdd(), request.getJobIdsToRemove(),
                c -> listener.onResponse(new PutCalendarAction.Response(c)), listener::onFailure);
    }
}
