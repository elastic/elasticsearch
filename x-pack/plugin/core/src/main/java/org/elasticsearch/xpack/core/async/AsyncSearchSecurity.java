/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.async;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.xpack.core.search.action.GetAsyncStatusAction;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesAction;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesRequest;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesResponse;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.privilege.ClusterPrivilegeResolver;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

public class AsyncSearchSecurity {

    private static final FetchSourceContext FETCH_HEADERS_FIELD_CONTEXT = FetchSourceContext.of(
        true,
        new String[] { AsyncTaskIndexService.HEADERS_FIELD },
        Strings.EMPTY_ARRAY
    );

    private final String indexName;
    private final SecurityContext securityContext;
    private final Client client;
    private final OriginSettingClient clientWithOrigin;

    public AsyncSearchSecurity(String indexName, SecurityContext securityContext, Client client, String origin) {
        this.securityContext = securityContext;
        this.client = client;
        this.clientWithOrigin = new OriginSettingClient(client, origin);
        this.indexName = indexName;
    }

    public void currentUserHasCancelTaskPrivilege(Consumer<Boolean> consumer) {
        hasClusterPrivilege(
            ClusterPrivilegeResolver.CANCEL_TASK.name(),
            ActionListener.wrap(consumer::accept, ex -> consumer.accept(false))
        );
    }

    public void currentUserCanSeeStatusOfAllSearches(ActionListener<Boolean> listener) {
        // If the user has access to the action by-name, then they can get the status of any async search
        hasClusterPrivilege(GetAsyncStatusAction.NAME, listener);
    }

    /**
     * Sends a {@link HasPrivilegesRequest} for one cluster privilege of the current user.
     * <p>
     * The caller has checked that {@link SecurityContext#getAuthentication()} is present. The listener receives the raw response,
     * and an exception it throws is not caught here. Callers do not agree on whether that exception is a failed lookup or a
     * failure of the action that runs after a successful one, so each caller keeps that decision.
     */
    public static void checkClusterPrivilege(
        SecurityContext securityContext,
        Client client,
        String privilegeName,
        ActionListener<HasPrivilegesResponse> listener
    ) {
        Authentication current = securityContext.getAuthentication();
        HasPrivilegesRequest request = new HasPrivilegesRequest();
        request.username(current.getEffectiveSubject().getUser().principal());
        request.clusterPrivileges(privilegeName);
        request.indexPrivileges(new RoleDescriptor.IndicesPrivileges[] {});
        request.applicationPrivileges(new RoleDescriptor.ApplicationResourcePrivileges[] {});
        client.execute(HasPrivilegesAction.INSTANCE, request, listener);
    }

    private void hasClusterPrivilege(String privilegeName, ActionListener<Boolean> listener) {
        if (securityContext.getAuthentication() != null) {
            try {
                checkClusterPrivilege(securityContext, client, privilegeName, listener.map(resp -> resp.isCompleteMatch()));
            } catch (Exception exc) {
                listener.onFailure(exc);
            }
        } else {
            listener.onResponse(false);
        }
    }

    public boolean currentUserHasAccessToTask(AsyncTask asyncTask) throws IOException {
        Objects.requireNonNull(asyncTask, "Task cannot be null");
        return currentUserHasAccessToTaskWithHeaders(asyncTask.getOriginHeaders());
    }

    public boolean currentUserHasAccessToTaskWithHeaders(Map<String, String> headers) throws IOException {
        return securityContext.canIAccessResourcesCreatedWithHeaders(headers);
    }

    /**
     * Checks if the current user can access the async search result of the original user.
     */
    void ensureAuthenticatedUserCanDeleteFromIndex(AsyncExecutionId executionId, ActionListener<Void> listener) {
        getTaskHeadersFromIndex(executionId, listener.map(headers -> {
            if (currentUserHasAccessToTaskWithHeaders(headers)) {
                return null;
            } else {
                throw new ResourceNotFoundException(executionId.getEncoded());
            }
        }));
    }

    private void getTaskHeadersFromIndex(AsyncExecutionId executionId, ActionListener<Map<String, String>> listener) {
        GetRequest internalGet = new GetRequest(indexName).preference(executionId.getEncoded())
            .id(executionId.getDocId())
            .fetchSourceContext(FETCH_HEADERS_FIELD_CONTEXT);

        clientWithOrigin.get(internalGet, ActionListener.wrap(get -> {
            if (get.isExists() == false) {
                listener.onFailure(new ResourceNotFoundException(executionId.getEncoded()));
                return;
            }
            // Check authentication for the user
            @SuppressWarnings("unchecked")
            Map<String, String> headers = (Map<String, String>) get.getSource().get(AsyncTaskIndexService.HEADERS_FIELD);
            listener.onResponse(headers);
        }, exc -> listener.onFailure(new ResourceNotFoundException(executionId.getEncoded()))));
    }

}
