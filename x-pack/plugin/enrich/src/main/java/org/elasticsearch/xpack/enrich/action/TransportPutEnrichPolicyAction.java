/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.enrich.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.AcknowledgedTransportMasterNodeProjectAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.enrich.action.PutEnrichPolicyAction;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesAction;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesRequest;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesResponse;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.support.Exceptions;
import org.elasticsearch.xpack.enrich.EnrichStore;

public class TransportPutEnrichPolicyAction extends AcknowledgedTransportMasterNodeProjectAction<PutEnrichPolicyAction.Request> {

    private final SecurityContext securityContext;
    private final Client client;
    private final Settings settings;
    private final IndexNameExpressionResolver indexNameExpressionResolver;

    @Inject
    public TransportPutEnrichPolicyAction(
        Settings settings,
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        Client client,
        ActionFilters actionFilters,
        ProjectResolver projectResolver,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            PutEnrichPolicyAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            PutEnrichPolicyAction.Request::new,
            projectResolver,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.settings = settings;
        this.securityContext = XPackSettings.SECURITY_ENABLED.get(settings)
            ? new SecurityContext(settings, threadPool.getThreadContext())
            : null;
        this.client = client;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
    }

    @Override
    protected void masterOperation(
        Task task,
        PutEnrichPolicyAction.Request request,
        ProjectState state,
        ActionListener<AcknowledgedResponse> listener
    ) {

        if (XPackSettings.SECURITY_ENABLED.get(settings)) {
            RoleDescriptor.IndicesPrivileges privileges = RoleDescriptor.IndicesPrivileges.builder()
                .indices(request.getPolicy().getIndices())
                .privileges("read")
                .build();

            String username = securityContext.getUser().principal();

            HasPrivilegesRequest privRequest = new HasPrivilegesRequest();
            privRequest.applicationPrivileges(new RoleDescriptor.ApplicationResourcePrivileges[0]);
            privRequest.username(username);
            privRequest.clusterPrivileges(Strings.EMPTY_ARRAY);
            privRequest.indexPrivileges(privileges);

            ActionListener<HasPrivilegesResponse> wrappedListener = listener.delegateFailureAndWrap((delegate, r) -> {
                if (r.isCompleteMatch()) {
                    putPolicy(state.projectId(), request, delegate);
                } else {
                    delegate.onFailure(
                        Exceptions.authorizationError(
                            "unable to store policy because no indices match with the " + "specified index patterns {}",
                            request.getPolicy().getIndices(),
                            username
                        )
                    );
                }
            });
            client.execute(HasPrivilegesAction.INSTANCE, privRequest, wrappedListener);
        } else {
            putPolicy(state.projectId(), request, listener);
        }
    }

    private void putPolicy(ProjectId projectId, PutEnrichPolicyAction.Request request, ActionListener<AcknowledgedResponse> listener) {
        EnrichStore.putPolicy(projectId, request.getName(), request.getPolicy(), clusterService, indexNameExpressionResolver, e -> {
            if (e == null) {
                listener.onResponse(AcknowledgedResponse.TRUE);
            } else {
                listener.onFailure(e);
            }
        });
    }

    @Override
    protected ClusterBlockException checkBlock(PutEnrichPolicyAction.Request request, ProjectState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
