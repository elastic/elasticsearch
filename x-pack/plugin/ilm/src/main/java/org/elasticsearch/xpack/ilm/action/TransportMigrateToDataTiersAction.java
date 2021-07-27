/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ilm.action;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.cluster.action.MigrateToDataTiersAction;
import org.elasticsearch.xpack.cluster.action.MigrateToDataTiersRequest;
import org.elasticsearch.xpack.cluster.action.MigrateToDataTiersResponse;
import org.elasticsearch.xpack.cluster.metadata.MetadataMigrateToDataTiersRoutingService.MigratedEntities;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleMetadata;

import static org.elasticsearch.xpack.cluster.metadata.MetadataMigrateToDataTiersRoutingService.migrateToDataTiersRouting;
import static org.elasticsearch.xpack.core.ilm.OperationMode.STOPPED;

public class TransportMigrateToDataTiersAction extends TransportMasterNodeAction<MigrateToDataTiersRequest, MigrateToDataTiersResponse> {

    private final NamedXContentRegistry xContentRegistry;
    private final Client client;
    private final XPackLicenseState licenseState;

    @Inject
    public TransportMigrateToDataTiersAction(TransportService transportService, ClusterService clusterService,
                                             ThreadPool threadPool, ActionFilters actionFilters,
                                             IndexNameExpressionResolver indexNameExpressionResolver,
                                             NamedXContentRegistry xContentRegistry, Client client, XPackLicenseState licenseState) {
        super(MigrateToDataTiersAction.NAME, transportService, clusterService, threadPool, actionFilters, MigrateToDataTiersRequest::new,
            indexNameExpressionResolver, MigrateToDataTiersResponse::new, ThreadPool.Names.SAME);
        this.xContentRegistry = xContentRegistry;
        this.client = client;
        this.licenseState = licenseState;
    }

    @Override
    protected void masterOperation(Task task, MigrateToDataTiersRequest request, ClusterState state,
                                   ActionListener<MigrateToDataTiersResponse> listener) throws Exception {
        IndexLifecycleMetadata currentMetadata = state.metadata().custom(IndexLifecycleMetadata.TYPE);
        if (currentMetadata != null && currentMetadata.getOperationMode() != STOPPED) {
            listener.onFailure(new IllegalStateException("stop ILM before migrating to data tiers, current state is [" +
                currentMetadata.getOperationMode() + "]"));
            return;
        }

        if (request.isDryRun()) {
            MigratedEntities entities =
                migrateToDataTiersRouting(state, request.getNodeAttributeName(), request.getLegacyTemplateToDelete(),
                    xContentRegistry, client, licenseState).v2();
            listener.onResponse(
                new MigrateToDataTiersResponse(entities.removedIndexTemplateName, entities.migratedPolicies, entities.migratedIndices, true)
            );
            return;
        }

        final SetOnce<MigratedEntities> migratedEntities = new SetOnce<>();
        clusterService.submitStateUpdateTask("migrate-to-data-tiers []", new ClusterStateUpdateTask(Priority.HIGH) {
            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                Tuple<ClusterState, MigratedEntities> migratedEntitiesTuple =
                    migrateToDataTiersRouting(state, request.getNodeAttributeName(), request.getLegacyTemplateToDelete(),
                        xContentRegistry, client, licenseState);

                migratedEntities.set(migratedEntitiesTuple.v2());
                return migratedEntitiesTuple.v1();
            }

            @Override
            public void onFailure(String source, Exception e) {
                listener.onFailure(e);
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                super.clusterStateProcessed(source, oldState, newState);
                MigratedEntities entities = migratedEntities.get();
                listener.onResponse(new MigrateToDataTiersResponse(entities.removedIndexTemplateName, entities.migratedPolicies,
                    entities.migratedIndices, false)
                );
            }
        });

    }

    @Override
    protected ClusterBlockException checkBlock(MigrateToDataTiersRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
