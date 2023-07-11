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
import org.elasticsearch.client.internal.ParentTaskAssigningClient;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.ml.MachineLearningField;
import org.elasticsearch.xpack.core.ml.action.PreviewDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.action.PreviewDataFrameAnalyticsAction.Request;
import org.elasticsearch.xpack.core.ml.action.PreviewDataFrameAnalyticsAction.Response;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.ml.dataframe.extractor.DataFrameDataExtractor;
import org.elasticsearch.xpack.ml.dataframe.extractor.DataFrameDataExtractorFactory;
import org.elasticsearch.xpack.ml.dataframe.extractor.ExtractedFieldsDetectorFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.xpack.ml.utils.SecondaryAuthorizationUtils.useSecondaryAuthIfAvailable;

/**
 * Previews the data that is sent to the analytics model for training
 */
public class TransportPreviewDataFrameAnalyticsAction extends HandledTransportAction<Request, Response> {

    private final XPackLicenseState licenseState;
    private final NodeClient client;
    private final SecurityContext securityContext;
    private final ThreadPool threadPool;
    private final Settings settings;
    private final ClusterService clusterService;

    @Inject
    public TransportPreviewDataFrameAnalyticsAction(
        TransportService transportService,
        ActionFilters actionFilters,
        NodeClient client,
        XPackLicenseState licenseState,
        Settings settings,
        ThreadPool threadPool,
        ClusterService clusterService
    ) {
        super(PreviewDataFrameAnalyticsAction.NAME, transportService, actionFilters, Request::new);
        this.client = Objects.requireNonNull(client);
        this.licenseState = licenseState;
        this.threadPool = threadPool;
        this.settings = settings;
        this.securityContext = XPackSettings.SECURITY_ENABLED.get(settings)
            ? new SecurityContext(settings, threadPool.getThreadContext())
            : null;
        this.clusterService = clusterService;
    }

    private static Map<String, Object> mergeRow(DataFrameDataExtractor.Row row, List<String> fieldNames) {
        return row.getValues() == null
            ? Collections.emptyMap()
            : IntStream.range(0, row.getValues().length).boxed().collect(Collectors.toMap(fieldNames::get, i -> row.getValues()[i]));
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
        if (MachineLearningField.ML_API_FEATURE.check(licenseState) == false) {
            listener.onFailure(LicenseUtils.newComplianceException(XPackField.MACHINE_LEARNING));
            return;
        }
        if (XPackSettings.SECURITY_ENABLED.get(settings)) {
            useSecondaryAuthIfAvailable(this.securityContext, () -> {
                // Set the auth headers (preferring the secondary headers) to the caller's.
                // Regardless if the config was previously stored or not.
                DataFrameAnalyticsConfig config = new DataFrameAnalyticsConfig.Builder(request.getConfig()).setHeaders(
                    ClientHelper.getPersistableSafeSecurityHeaders(threadPool.getThreadContext(), clusterService.state())
                ).build();
                preview(task, config, listener);
            });
        } else {
            preview(task, request.getConfig(), listener);
        }
    }

    void preview(Task task, DataFrameAnalyticsConfig config, ActionListener<Response> listener) {
        final TaskId parentTaskId = new TaskId(clusterService.localNode().getId(), task.getId());
        final ExtractedFieldsDetectorFactory extractedFieldsDetectorFactory = new ExtractedFieldsDetectorFactory(
            new ParentTaskAssigningClient(client, parentTaskId)
        );
        extractedFieldsDetectorFactory.createFromSource(config, listener.delegateFailureAndWrap((delegate, extractedFieldsDetector) -> {
            DataFrameDataExtractor extractor = DataFrameDataExtractorFactory.createForSourceIndices(
                client,
                parentTaskId.toString(),
                config,
                extractedFieldsDetector.detect().v1()
            ).newExtractor(false);
            extractor.preview(delegate.delegateFailureAndWrap((l, rows) -> {
                List<String> fieldNames = extractor.getFieldNames();
                l.onResponse(new Response(rows.stream().map((r) -> mergeRow(r, fieldNames)).collect(Collectors.toList())));
            }));
        }));
    }

}
