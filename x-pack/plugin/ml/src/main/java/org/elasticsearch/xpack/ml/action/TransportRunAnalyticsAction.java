package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.env.Environment;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.action.RunAnalyticsAction;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.analytics.DataFrameDataExtractor;
import org.elasticsearch.xpack.ml.analytics.DataFrameDataExtractorFactory;
import org.elasticsearch.xpack.ml.analytics.process.AnalyticsProcessManager;

import java.util.Collections;
import java.util.Map;
import java.util.function.Supplier;

public class TransportRunAnalyticsAction extends HandledTransportAction<RunAnalyticsAction.Request, AcknowledgedResponse> {

    private final TransportService transportService;
    private final ThreadPool threadPool;
    private final Client client;
    private final ClusterService clusterService;
    private final Environment environment;
    private final AnalyticsProcessManager analyticsProcessManager;


    @Inject
    public TransportRunAnalyticsAction(ThreadPool threadPool, TransportService transportService, ActionFilters actionFilters,
                                       Client client, ClusterService clusterService, Environment environment,
                                       AnalyticsProcessManager analyticsProcessManager) {
        super(RunAnalyticsAction.NAME, transportService, actionFilters,
            (Supplier<RunAnalyticsAction.Request>) RunAnalyticsAction.Request::new);
        this.transportService = transportService;
        this.threadPool = threadPool;
        this.client = client;
        this.clusterService = clusterService;
        this.environment = environment;
        this.analyticsProcessManager = analyticsProcessManager;
    }

    @Override
    protected void doExecute(Task task, RunAnalyticsAction.Request request, ActionListener<AcknowledgedResponse> listener) {
        DiscoveryNode localNode = clusterService.localNode();
        if (isMlNode(localNode)) {
            runPipelineAnalytics(request, listener);
            return;
        }

        ClusterState clusterState = clusterService.state();
        for (DiscoveryNode node : clusterState.getNodes()) {
            if (isMlNode(node)) {
                transportService.sendRequest(node, actionName, request,
                    new ActionListenerResponseHandler<>(listener, inputStream -> {
                            AcknowledgedResponse response = new AcknowledgedResponse();
                            response.readFrom(inputStream);
                            return response;
                    }));
                return;
            }
        }
        listener.onFailure(ExceptionsHelper.badRequestException("No ML node to run on"));
    }

    private boolean isMlNode(DiscoveryNode node) {
        Map<String, String> nodeAttributes = node.getAttributes();
        String enabled = nodeAttributes.get(MachineLearning.ML_ENABLED_NODE_ATTR);
        return Boolean.valueOf(enabled);
    }

    private void runPipelineAnalytics(RunAnalyticsAction.Request request, ActionListener<AcknowledgedResponse> listener) {
        String jobId = "ml-analytics-" + request.getIndex();

        ActionListener<DataFrameDataExtractorFactory> dataExtractorFactoryListener = ActionListener.wrap(
            dataExtractorFactory -> {
                DataFrameDataExtractor dataExtractor = dataExtractorFactory.newExtractor();
                analyticsProcessManager.processData(jobId, dataExtractor);
                listener.onResponse(new AcknowledgedResponse(true));
            },
            listener::onFailure
        );

        DataFrameDataExtractorFactory.create(client, Collections.emptyMap(), request.getIndex(), dataExtractorFactoryListener);
    }

}
