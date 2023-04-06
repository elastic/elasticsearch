/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ilm.action;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.cluster.action.MigrateToDataTiersAction;
import org.elasticsearch.xpack.cluster.action.MigrateToDataTiersRequest;
import org.elasticsearch.xpack.cluster.action.MigrateToDataTiersResponse;
import org.elasticsearch.xpack.cluster.metadata.MetadataMigrateToDataTiersRoutingService.MigratedEntities;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleMetadata;

import static org.elasticsearch.xpack.cluster.metadata.MetadataMigrateToDataTiersRoutingService.migrateToDataTiersRouting;
import static org.elasticsearch.xpack.core.ilm.LifecycleOperationMetadata.currentILMMode;
import static org.elasticsearch.xpack.core.ilm.OperationMode.STOPPED;

public class TransportMigrateToDataTiersAction extends TransportMasterNodeAction<MigrateToDataTiersRequest, MigrateToDataTiersResponse> {

    private static final Logger logger = LogManager.getLogger(TransportMigrateToDataTiersAction.class);

    private final NamedXContentRegistry xContentRegistry;
    private final Client client;
    private final XPackLicenseState licenseState;

    @Inject
    public TransportMigrateToDataTiersAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        NamedXContentRegistry xContentRegistry,
        Client client,
        XPackLicenseState licenseState
    ) {
        super(
            MigrateToDataTiersAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            MigrateToDataTiersRequest::new,
            indexNameExpressionResolver,
            MigrateToDataTiersResponse::new,
            ThreadPool.Names.SAME
        );
        this.xContentRegistry = xContentRegistry;
        this.client = client;
        this.licenseState = licenseState;
    }

    @Override
    protected void masterOperation(
        Task task,
        MigrateToDataTiersRequest request,
        ClusterState state,
        ActionListener<MigrateToDataTiersResponse> listener
    ) throws Exception {
        if (request.isDryRun()) {
            MigratedEntities entities = migrateToDataTiersRouting(
                state,
                request.getNodeAttributeName(),
                request.getLegacyTemplateToDelete(),
                xContentRegistry,
                client,
                licenseState,
                request.isDryRun()
            ).v2();
            listener.onResponse(
                new MigrateToDataTiersResponse(
                    entities.removedIndexTemplateName,
                    entities.migratedPolicies,
                    entities.migratedIndices,
                    entities.migratedTemplates.migratedLegacyTemplates,
                    entities.migratedTemplates.migratedComposableTemplates,
                    entities.migratedTemplates.migratedComponentTemplates,
                    true
                )
            );
            return;
        }

        IndexLifecycleMetadata currentMetadata = state.metadata().custom(IndexLifecycleMetadata.TYPE);
        if (currentMetadata != null && currentILMMode(state) != STOPPED) {
            listener.onFailure(
                new IllegalStateException("stop ILM before migrating to data tiers, current state is [" + currentILMMode(state) + "]")
            );
            return;
        }

        final SetOnce<MigratedEntities> migratedEntities = new SetOnce<>();
        submitUnbatchedTask("migrate-to-data-tiers []", new ClusterStateUpdateTask(Priority.HIGH) {
            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                Tuple<ClusterState, MigratedEntities> migratedEntitiesTuple = migrateToDataTiersRouting(
                    state,
                    request.getNodeAttributeName(),
                    request.getLegacyTemplateToDelete(),
                    xContentRegistry,
                    client,
                    licenseState,
                    request.isDryRun()
                );

                migratedEntities.set(migratedEntitiesTuple.v2());
                return migratedEntitiesTuple.v1();
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }

            @Override
            public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
                clusterService.getRerouteService()
                    .reroute("cluster migrated to data tiers routing", Priority.NORMAL, new ActionListener<Void>() {
                        @Override
                        public void onResponse(Void ignored) {}

                        @Override
                        public void onFailure(Exception e) {
                            logger.log(
                                MasterService.isPublishFailureException(e) ? Level.DEBUG : Level.WARN,
                                "unsuccessful reroute after migration to data tiers routing",
                                e
                            );
                        }
                    });
                MigratedEntities entities = migratedEntities.get();
                listener.onResponse(
                    new MigrateToDataTiersResponse(
                        entities.removedIndexTemplateName,
                        entities.migratedPolicies,
                        entities.migratedIndices,
                        entities.migratedTemplates.migratedLegacyTemplates,
                        entities.migratedTemplates.migratedComposableTemplates,
                        entities.migratedTemplates.migratedComponentTemplates,
                        false
                    )
                );
            }
        });

    }

    @SuppressForbidden(reason = "legacy usage of unbatched task") // TODO add support for batching here
    private void submitUnbatchedTask(@SuppressWarnings("SameParameterValue") String source, ClusterStateUpdateTask task) {
        clusterService.submitUnbatchedStateUpdateTask(source, task);
    }

    @Override
    protected ClusterBlockException checkBlock(MigrateToDataTiersRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
