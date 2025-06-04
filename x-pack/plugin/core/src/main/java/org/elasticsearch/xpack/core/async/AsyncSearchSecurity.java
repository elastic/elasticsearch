/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.async;

import org.elasticsearch.exception.ResourceNotFoundException;
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

    private void hasClusterPrivilege(String privilegeName, ActionListener<Boolean> listener) {
        final Authentication current = securityContext.getAuthentication();
        if (current != null) {
            HasPrivilegesRequest req = new HasPrivilegesRequest();
            req.username(current.getEffectiveSubject().getUser().principal());
            req.clusterPrivileges(privilegeName);
            req.indexPrivileges(new RoleDescriptor.IndicesPrivileges[] {});
            req.applicationPrivileges(new RoleDescriptor.ApplicationResourcePrivileges[] {});
            try {
                client.execute(HasPrivilegesAction.INSTANCE, req, listener.map(resp -> resp.isCompleteMatch()));
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
