/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.enrich.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
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

public class TransportPutEnrichPolicyAction extends HandledTransportAction<PutEnrichPolicyAction.Request, AcknowledgedResponse> {

    private final XPackLicenseState licenseState;
    private final SecurityContext securityContext;
    private final Client client;
    private final ClusterService clusterService;

    @Inject
    public TransportPutEnrichPolicyAction(Settings settings, TransportService transportService,
                                          ClusterService clusterService, ThreadPool threadPool, Client client,
                                          XPackLicenseState licenseState, ActionFilters actionFilters) {
        super(PutEnrichPolicyAction.NAME, transportService, actionFilters, PutEnrichPolicyAction.Request::new);

        this.licenseState = licenseState;
        this.securityContext = XPackSettings.SECURITY_ENABLED.get(settings) ?
            new SecurityContext(settings, threadPool.getThreadContext()) : null;
        this.client = client;
        this.clusterService = clusterService;
    }

    protected AcknowledgedResponse newResponse() {
        throw new UnsupportedOperationException("usage of Streamable is to be replaced by Writeable");
    }

    @Override
    protected void doExecute(Task task, PutEnrichPolicyAction.Request request, ActionListener<AcknowledgedResponse> listener) {
        if (licenseState.isAuthAllowed()) {
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

            ActionListener<HasPrivilegesResponse> wrappedListener = ActionListener.wrap(
                r -> {
                    if (r.isCompleteMatch()) {
                        putPolicy(request, listener);
                    } else {
                        listener.onFailure(Exceptions.authorizationError("unable to store policy because no indices match with the " +
                            "specified index patterns {}", request.getPolicy().getIndices(), username));
                    }
                },
                listener::onFailure);
            client.execute(HasPrivilegesAction.INSTANCE, privRequest, wrappedListener);
        } else {
            putPolicy(request, listener);
        }
    }

    private void putPolicy(PutEnrichPolicyAction.Request request, ActionListener<AcknowledgedResponse> listener ) {
        EnrichStore.putPolicy(request.getName(), request.getPolicy(), clusterService, client, e -> {
            if (e == null) {
                listener.onResponse(new AcknowledgedResponse(true));
            } else {
                listener.onFailure(e);
            }
        });
    }
}
