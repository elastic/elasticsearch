/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.transform.action;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ingest.SimulatePipelineAction;
import org.elasticsearch.action.ingest.SimulatePipelineRequest;
import org.elasticsearch.action.ingest.SimulatePipelineResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.license.License;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.RemoteClusterLicenseChecker;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.common.validation.SourceDestValidator;
import org.elasticsearch.xpack.core.transform.TransformField;
import org.elasticsearch.xpack.core.transform.action.PreviewTransformAction;
import org.elasticsearch.xpack.core.transform.transforms.SourceConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformDestIndexSettings;
import org.elasticsearch.xpack.transform.persistence.TransformIndex;
import org.elasticsearch.xpack.transform.transforms.Function;
import org.elasticsearch.xpack.transform.transforms.FunctionFactory;
import org.elasticsearch.xpack.transform.utils.SourceDestValidations;

import java.time.Clock;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class TransportPreviewTransformAction extends HandledTransportAction<
    PreviewTransformAction.Request,
    PreviewTransformAction.Response> {

    private static final Logger logger = LogManager.getLogger(TransportPreviewTransformAction.class);
    private static final int NUMBER_OF_PREVIEW_BUCKETS = 100;
    private final XPackLicenseState licenseState;
    private final Client client;
    private final ThreadPool threadPool;
    private final ClusterService clusterService;
    private final SourceDestValidator sourceDestValidator;

    @Inject
    public TransportPreviewTransformAction(
        TransportService transportService,
        ActionFilters actionFilters,
        Client client,
        ThreadPool threadPool,
        XPackLicenseState licenseState,
        IndexNameExpressionResolver indexNameExpressionResolver,
        ClusterService clusterService,
        Settings settings
    ) {
        this(
            PreviewTransformAction.NAME,
            transportService,
            actionFilters,
            client,
            threadPool,
            licenseState,
            indexNameExpressionResolver,
            clusterService,
            settings
        );
    }

    protected TransportPreviewTransformAction(
        String name,
        TransportService transportService,
        ActionFilters actionFilters,
        Client client,
        ThreadPool threadPool,
        XPackLicenseState licenseState,
        IndexNameExpressionResolver indexNameExpressionResolver,
        ClusterService clusterService,
        Settings settings
    ) {
        super(name, transportService, actionFilters, PreviewTransformAction.Request::new);
        this.licenseState = licenseState;
        this.client = client;
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.sourceDestValidator = new SourceDestValidator(
            indexNameExpressionResolver,
            transportService.getRemoteClusterService(),
            DiscoveryNode.isRemoteClusterClient(settings)
                ? new RemoteClusterLicenseChecker(client, XPackLicenseState::isTransformAllowedForOperationMode)
                : null,
            clusterService.getNodeName(),
            License.OperationMode.BASIC.description()
        );
    }

    @Override
    protected void doExecute(Task task, PreviewTransformAction.Request request, ActionListener<PreviewTransformAction.Response> listener) {
        if (!licenseState.checkFeature(XPackLicenseState.Feature.TRANSFORM)) {
            listener.onFailure(LicenseUtils.newComplianceException(XPackField.TRANSFORM));
            return;
        }

        ClusterState clusterState = clusterService.state();

        final TransformConfig config = request.getConfig();

        sourceDestValidator.validate(
            clusterState,
            config.getSource().getIndex(),
            config.getDestination().getIndex(),
            SourceDestValidations.PREVIEW_VALIDATIONS,
            ActionListener.wrap(r -> {
                // create the function for validation
                final Function function = FunctionFactory.create(config);
                function.validateConfig(ActionListener.wrap(functionValidationResponse -> {
                    getPreview(
                        config.getId(), // note: @link{PreviewTransformAction} sets an id, so this is never null
                        function,
                        config.getSource(),
                        config.getDestination().getPipeline(),
                        config.getDestination().getIndex(),
                        listener
                    );
                }, listener::onFailure));
            }, listener::onFailure)
        );
    }

    @SuppressWarnings("unchecked")
    private void getPreview(
        String transformId,
        Function function,
        SourceConfig source,
        String pipeline,
        String dest,
        ActionListener<PreviewTransformAction.Response> listener
    ) {
        final SetOnce<Map<String, String>> mappings = new SetOnce<>();

        ActionListener<SimulatePipelineResponse> pipelineResponseActionListener = ActionListener.wrap(simulatePipelineResponse -> {
            List<Map<String, Object>> docs = new ArrayList<>(simulatePipelineResponse.getResults().size());
            for (var simulateDocumentResult : simulatePipelineResponse.getResults()) {
                try (XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()) {
                    XContentBuilder content = simulateDocumentResult.toXContent(xContentBuilder, ToXContent.EMPTY_PARAMS);
                    Map<String, Object> tempMap = XContentHelper.convertToMap(BytesReference.bytes(content), true, XContentType.JSON).v2();
                    docs.add((Map<String, Object>) XContentMapValues.extractValue("doc._source", tempMap));
                }
            }
            TransformDestIndexSettings generatedDestIndexSettings = TransformIndex.createTransformDestIndexSettings(
                mappings.get(),
                transformId,
                Clock.systemUTC()
            );

            listener.onResponse(new PreviewTransformAction.Response(docs, generatedDestIndexSettings));
        }, listener::onFailure);
        function.deduceMappings(client, source, ActionListener.wrap(deducedMappings -> {
            mappings.set(deducedMappings);
            function.preview(
                client,
                threadPool.getThreadContext().getHeaders(),
                source,
                deducedMappings,
                NUMBER_OF_PREVIEW_BUCKETS,
                ActionListener.wrap(docs -> {
                    if (pipeline == null) {
                        TransformDestIndexSettings generatedDestIndexSettings = TransformIndex.createTransformDestIndexSettings(
                            mappings.get(),
                            transformId,
                            Clock.systemUTC()
                        );

                        listener.onResponse(new PreviewTransformAction.Response(docs, generatedDestIndexSettings));
                    } else {
                        List<Map<String, Object>> results = docs.stream().map(doc -> {
                            Map<String, Object> src = new HashMap<>();
                            String id = (String) doc.get(TransformField.DOCUMENT_ID_FIELD);
                            src.put("_source", doc);
                            src.put("_id", id);
                            src.put("_index", dest);
                            return src;
                        }).collect(Collectors.toList());

                        try (XContentBuilder builder = jsonBuilder()) {
                            builder.startObject();
                            builder.field("docs", results);
                            builder.endObject();
                            var pipelineRequest = new SimulatePipelineRequest(BytesReference.bytes(builder), XContentType.JSON);
                            pipelineRequest.setId(pipeline);
                            ClientHelper.executeAsyncWithOrigin(
                                client,
                                ClientHelper.TRANSFORM_ORIGIN,
                                SimulatePipelineAction.INSTANCE,
                                pipelineRequest,
                                pipelineResponseActionListener
                            );
                        }
                    }
                }, listener::onFailure)
            );

        }, listener::onFailure));
    }
}
