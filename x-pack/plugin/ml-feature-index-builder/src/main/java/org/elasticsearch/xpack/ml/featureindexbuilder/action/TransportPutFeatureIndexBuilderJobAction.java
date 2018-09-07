/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.featureindexbuilder.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.ml.featureindexbuilder.action.PutFeatureIndexBuilderJobAction.Request;
import org.elasticsearch.xpack.ml.featureindexbuilder.action.PutFeatureIndexBuilderJobAction.Response;
import org.elasticsearch.xpack.ml.featureindexbuilder.job.FeatureIndexBuilderJob;
import org.elasticsearch.xpack.ml.featureindexbuilder.job.FeatureIndexBuilderJobConfig;

import static org.elasticsearch.xpack.core.ml.job.persistence.ElasticsearchMappings.DOC_TYPE;

public class TransportPutFeatureIndexBuilderJobAction
        extends TransportMasterNodeAction<PutFeatureIndexBuilderJobAction.Request, PutFeatureIndexBuilderJobAction.Response> {

    // TODO: hack, to be replaced
    private static final String PIVOT_INDEX = "pivot-reviews";

    private final XPackLicenseState licenseState;
    private final PersistentTasksService persistentTasksService;
    private final Client client;

    @Inject
    public TransportPutFeatureIndexBuilderJobAction(Settings settings, TransportService transportService, ThreadPool threadPool,
            ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver, ClusterService clusterService,
            XPackLicenseState licenseState, PersistentTasksService persistentTasksService, Client client) {
        super(settings, PutFeatureIndexBuilderJobAction.NAME, transportService, clusterService, threadPool, actionFilters,
                indexNameExpressionResolver, PutFeatureIndexBuilderJobAction.Request::new);
        this.licenseState = licenseState;
        this.persistentTasksService = persistentTasksService;
        this.client = client;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected PutFeatureIndexBuilderJobAction.Response newResponse() {
        return new PutFeatureIndexBuilderJobAction.Response();
    }

    @Override
    protected void masterOperation(Request request, ClusterState clusterState, ActionListener<Response> listener) throws Exception {

        if (!licenseState.isMachineLearningAllowed()) {
            listener.onFailure(LicenseUtils.newComplianceException(XPackField.FIB));
            return;
        }

        XPackPlugin.checkReadyForXPackCustomMetadata(clusterState);

        FeatureIndexBuilderJob job = createFeatureIndexBuilderJob(request.getConfig(), threadPool);
        createIndex(client, job.getConfig().getId());
        startPersistentTask(job, listener, persistentTasksService);
    }

    private static FeatureIndexBuilderJob createFeatureIndexBuilderJob(FeatureIndexBuilderJobConfig config, ThreadPool threadPool) {
        return new FeatureIndexBuilderJob(config);
    }

    static void startPersistentTask(FeatureIndexBuilderJob job, ActionListener<PutFeatureIndexBuilderJobAction.Response> listener,
            PersistentTasksService persistentTasksService) {

        persistentTasksService.sendStartRequest(job.getConfig().getId(), FeatureIndexBuilderJob.NAME, job,
                ActionListener.wrap(persistentTask -> {
                    listener.onResponse(new PutFeatureIndexBuilderJobAction.Response(true));
                }, e -> {
                    listener.onFailure(e);
                }));
    }

    @Override
    protected ClusterBlockException checkBlock(PutFeatureIndexBuilderJobAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    /*
     * Mocked demo case
     *
     * TODO: everything below will be replaced with proper implementation read from job configuration 
     */
    private static void createIndex(Client client, String suffix) {

        String indexName = PIVOT_INDEX + "_" + suffix;
        CreateIndexRequest request = new CreateIndexRequest(indexName);

        request.settings(Settings.builder() // <1>
                .put("index.number_of_shards", 1).put("index.number_of_replicas", 0));
        request.mapping(DOC_TYPE, // <1>
                "{\n" +
                "  \"" + DOC_TYPE + "\": {\n" +
                "    \"properties\": {\n" +
                "      \"reviewerId\": {\n" +
                "        \"type\": \"keyword\"\n" +
                "      },\n" +
                "      \"avg_rating\": {\n" +
                "        \"type\": \"integer\"\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}", // <2>
                XContentType.JSON);
        IndicesAdminClient adminClient = client.admin().indices();
        adminClient.create(request).actionGet();
    }
}
