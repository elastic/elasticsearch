/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.view;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.AcknowledgedTransportMasterNodeProjectAction;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.View;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.Subject;

public class TransportPutViewAction extends AcknowledgedTransportMasterNodeProjectAction<PutViewAction.Request> {
    private final ViewService viewService;
    private final ThreadPool threadPool;

    @Inject
    public TransportPutViewAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        ViewService viewService,
        ProjectResolver projectResolver
    ) {
        super(
            PutViewAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            PutViewAction.Request::new,
            projectResolver,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.viewService = viewService;
        this.threadPool = threadPool;
    }

    @Override
    protected void masterOperation(
        Task task,
        PutViewAction.Request request,
        ProjectState state,
        ActionListener<AcknowledgedResponse> listener
    ) {
        viewService.putView(state.projectId(), request, captureDefiner(request), listener);
    }

    /**
     * For a definer-rights view, capture the creator's identity from the authentication on the calling thread so the stored view
     * runs (in a later phase) as the user who created it. The identity is taken from the {@link SecurityContext}, never from the
     * request body, so a caller cannot forge a definer. Invoker-rights views (the default and every existing view) capture nothing.
     * <p>
     * Only the definer principal (username + realm) is captured here. The full resolved role-descriptor set requires the security
     * plugin's authorization service, which is not wired into this action; until definer execution is implemented the role
     * descriptors are stored empty. When {@code null} is returned for a definer request — security disabled, no authentication on
     * the thread — the view is still stored as definer-mode but with no identity, and remains unqueryable (rejected at resolution).
     */
    private View.DefinerInfo captureDefiner(PutViewAction.Request request) {
        if (request.view().rightsMode() != View.RightsMode.DEFINER) {
            return null;
        }
        final ThreadContext threadContext = threadPool.getThreadContext();
        final Authentication authentication = new SecurityContext(Settings.EMPTY, threadContext).getAuthentication();
        if (authentication == null) {
            return null;
        }
        final Subject subject = authentication.getEffectiveSubject();
        final String realmName = subject.getRealm() == null ? "" : subject.getRealm().getName();
        return new View.DefinerInfo(subject.getUser().principal(), realmName, BytesArray.EMPTY);
    }

    @Override
    protected ClusterBlockException checkBlock(PutViewAction.Request request, ProjectState state) {
        return state.blocks().globalBlockedException(state.projectId(), ClusterBlockLevel.METADATA_WRITE);
    }
}
