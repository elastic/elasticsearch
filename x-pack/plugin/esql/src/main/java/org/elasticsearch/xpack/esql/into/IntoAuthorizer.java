/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.into;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesAction;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesRequest;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;

/**
 * Confirms the caller may write to an {@code INTO} destination before {@link IntoDestination} looks at it. The check of
 * the destination reads cluster state, which is not subject to authorization, so without this step a caller with no
 * privileges on a name could tell from a verification error whether it exists and which fields its mapping holds.
 * <p>
 * The question goes to the security plugin as the caller (a {@code has_privileges} request for their own user), and asks
 * for the privilege the writes will need: {@code create_doc} for the {@code create} operations of {@code abort} and
 * {@code keep}, {@code index} for the overwrites of {@code replace}. Names that do not exist yet are checked by name,
 * as the bulk will. Without security the request carries no authentication and there is nothing to check.
 */
public final class IntoAuthorizer {

    private IntoAuthorizer() {}

    /** Completes {@code listener} when the caller may write to {@code destination}, or fails it with a 403. */
    public static void authorize(Client client, String destination, IntoOptions options, ActionListener<Void> listener) {
        Authentication authentication = new SecurityContext(Settings.EMPTY, client.threadPool().getThreadContext()).getAuthentication();
        if (authentication == null) {
            listener.onResponse(null);
            return;
        }
        String user = authentication.getEffectiveSubject().getUser().principal();
        String privilege = options.collisions() == IntoOptions.Collisions.REPLACE ? "index" : "create_doc";
        HasPrivilegesRequest request = new HasPrivilegesRequest();
        request.username(user);
        request.clusterPrivileges(Strings.EMPTY_ARRAY);
        request.indexPrivileges(RoleDescriptor.IndicesPrivileges.builder().indices(destination).privileges(privilege).build());
        request.applicationPrivileges(new RoleDescriptor.ApplicationResourcePrivileges[0]);
        client.execute(HasPrivilegesAction.INSTANCE, request, listener.delegateFailureAndWrap((l, response) -> {
            if (response.isCompleteMatch()) {
                l.onResponse(null);
            } else {
                l.onFailure(
                    new ElasticsearchSecurityException(
                        "action [indices:data/write/bulk] is unauthorized for user [{}] on index [{}], this action is granted by "
                            + "the index privileges [{},write,all]",
                        RestStatus.FORBIDDEN,
                        user,
                        destination,
                        privilege
                    )
                );
            }
        }));
    }
}
