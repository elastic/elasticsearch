/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.streaming;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.core.Nullable;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * Checks that the current user may read or delete a stream, the counterpart of {@code AsyncSearchSecurity}.
 * <p>
 * The rule is the one async results use: the stored security headers of the creator are compared with the current authentication
 * through {@link SecurityContext#canIAccessResourcesCreatedWithHeaders}. {@code AsyncSearchSecurity} itself is not reused because
 * its constructor needs an index name and an origin that streams do not have.
 */
public class StreamSearchSecurity {

    private final SecurityContext securityContext;
    private final Client client;

    /**
     * @param client used to ask whether the current user has a cluster privilege, see {@link #currentUserHasCancelTaskPrivilege}
     */
    public StreamSearchSecurity(SecurityContext securityContext, Client client) {
        this.securityContext = Objects.requireNonNull(securityContext);
        this.client = Objects.requireNonNull(client);
    }

    /**
     * Tells whether the current user may access a stream that was created with these security headers: the creator may, and so may
     * everyone if the headers hold no authentication, which means the stream was created with security off. See the TODO on
     * {@link #getStreamAndCheckAuthentication(String, StreamMetadata, long)} for what that asks of the writer.
     *
     * @throws IOException if the stored authentication cannot be read
     */
    public boolean currentUserHasAccessToStreamWithHeaders(Map<String, String> headers) throws IOException {
        return securityContext.canIAccessResourcesCreatedWithHeaders(headers);
    }

    /**
     * Tells whether the current user has the {@code cancel_task} cluster privilege, which allows deleting the streams of other
     * users, as it allows deleting their async results. A request that is not authenticated, and a lookup that fails, count as
     * "no privilege", as in {@code AsyncSearchSecurity}.
     */
    public void currentUserHasCancelTaskPrivilege(Consumer<Boolean> consumer) {
        final Authentication current = securityContext.getAuthentication();
        if (current == null) {
            consumer.accept(false);
            return;
        }
        HasPrivilegesRequest request = new HasPrivilegesRequest();
        request.username(current.getEffectiveSubject().getUser().principal());
        request.clusterPrivileges(ClusterPrivilegeResolver.CANCEL_TASK.name());
        request.indexPrivileges(new RoleDescriptor.IndicesPrivileges[] {});
        request.applicationPrivileges(new RoleDescriptor.ApplicationResourcePrivileges[] {});
        // The consumer gets one answer, the first one. ActionListener#wrap is not used: it would call the consumer again, with
        // false, if the consumer itself threw, and so run whatever it guards a second time. The action that answers may do the
        // same with an exception of the consumer, so a second answer is dropped.
        final AtomicBoolean answered = new AtomicBoolean();
        try {
            client.execute(HasPrivilegesAction.INSTANCE, request, new ActionListener<>() {
                @Override
                public void onResponse(HasPrivilegesResponse response) {
                    if (answered.compareAndSet(false, true)) {
                        consumer.accept(response.isCompleteMatch());
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    if (answered.compareAndSet(false, true)) {
                        consumer.accept(false);
                    }
                }
            });
        } catch (Exception e) {
            if (answered.compareAndSet(false, true) == false) {
                throw e;
            }
            consumer.accept(false);
        }
    }

    /**
     * TODO: the headers checked here are the ones the stream POST stores with {@link StreamStorage#create}. The POST does not store
     * streams yet. It must pass the security headers of the original request (the authentication header), and the real storage must
     * keep them. Empty headers are treated as "created with security off" and allow every caller, so a writer that passes an empty map
     * on a secured cluster would make the stream readable and deletable by everyone. Add a check or an assertion on the writer side.
     * <p>
     * Returns {@code metadata} when it exists, has not expired and was created by a user the current user may access.
     * Otherwise throws a {@link ResourceNotFoundException} with the same message in every case, so a caller cannot tell an unknown
     * stream from a stream that belongs to someone else.
     */
    public StreamMetadata getStreamAndCheckAuthentication(String id, @Nullable StreamMetadata metadata, long nowMillis) throws IOException {
        if (metadata == null || metadata.expiration().toEpochMilli() < nowMillis) {
            throw new ResourceNotFoundException(id);
        }
        return getStreamAndCheckAuthentication(id, metadata);
    }

    /**
     * The same check without the expiry: a stream that has expired but is still stored can be deleted by its owner, as an expired
     * async result can. The TODO on the other overload applies here too.
     */
    public StreamMetadata getStreamAndCheckAuthentication(String id, @Nullable StreamMetadata metadata) throws IOException {
        if (metadata == null || currentUserHasAccessToStreamWithHeaders(metadata.headers()) == false) {
            throw new ResourceNotFoundException(id);
        }
        return metadata;
    }
}
