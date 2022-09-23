/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.action.profile;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.action.user.ProfileHasPrivilegesAction;
import org.elasticsearch.xpack.core.security.action.user.ProfileHasPrivilegesRequest;
import org.elasticsearch.xpack.core.security.action.user.ProfileHasPrivilegesResponse;
import org.elasticsearch.xpack.core.security.authc.Subject;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilegeDescriptor;
import org.elasticsearch.xpack.security.authz.AuthorizationService;
import org.elasticsearch.xpack.security.authz.store.NativePrivilegeStore;
import org.elasticsearch.xpack.security.profile.ProfileService;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Transport action that tests whether the users for the given profile ids have the specified
 * {@link AuthorizationEngine.PrivilegesToCheck privileges}
 */
public class TransportProfileHasPrivilegesAction extends HandledTransportAction<ProfileHasPrivilegesRequest, ProfileHasPrivilegesResponse> {

    private static final Logger logger = LogManager.getLogger(TransportProfileHasPrivilegesAction.class);

    private final AuthorizationService authorizationService;
    private final NativePrivilegeStore privilegeStore;
    private final ProfileService profileService;
    private final SecurityContext securityContext;
    private final ThreadPool threadPool;

    @Inject
    public TransportProfileHasPrivilegesAction(
        TransportService transportService,
        ActionFilters actionFilters,
        AuthorizationService authorizationService,
        NativePrivilegeStore privilegeStore,
        ProfileService profileService,
        SecurityContext securityContext,
        ThreadPool threadPool
    ) {
        super(ProfileHasPrivilegesAction.NAME, transportService, actionFilters, ProfileHasPrivilegesRequest::new);
        this.authorizationService = authorizationService;
        this.privilegeStore = privilegeStore;
        this.profileService = profileService;
        this.securityContext = securityContext;
        this.threadPool = threadPool;
    }

    @Override
    protected void doExecute(Task task, ProfileHasPrivilegesRequest request, ActionListener<ProfileHasPrivilegesResponse> listener) {
        assert task instanceof CancellableTask : "task must be cancellable";
        profileService.getProfileSubjects(request.profileUids(), ActionListener.wrap(profileSubjectsAndFailures -> {
            if (profileSubjectsAndFailures.results().isEmpty()) {
                listener.onResponse(new ProfileHasPrivilegesResponse(Set.of(), profileSubjectsAndFailures.errors()));
                return;
            }
            final Set<String> hasPrivilegeProfiles = Collections.synchronizedSet(new HashSet<>());
            final Map<String, Exception> errorProfiles = new ConcurrentHashMap<>(profileSubjectsAndFailures.errors());
            final AtomicInteger counter = new AtomicInteger(profileSubjectsAndFailures.results().size());
            assert counter.get() > 0;
            resolveApplicationPrivileges(
                request,
                ActionListener.wrap(applicationPrivilegeDescriptors -> threadPool.generic().execute(() -> {
                    for (Map.Entry<String, Subject> profileUidToSubject : profileSubjectsAndFailures.results()) {
                        // return the partial response if the "has privilege" task got cancelled in the meantime
                        if (((CancellableTask) task).isCancelled()) {
                            listener.onFailure(new TaskCancelledException("has privilege task cancelled"));
                            return;
                        }
                        final String profileUid = profileUidToSubject.getKey();
                        final Subject subject = profileUidToSubject.getValue();
                        authorizationService.checkPrivileges(
                            subject,
                            request.privilegesToCheck(),
                            applicationPrivilegeDescriptors,
                            ActionListener.runAfter(ActionListener.wrap(privilegesCheckResult -> {
                                assert privilegesCheckResult.getDetails() == null;
                                if (privilegesCheckResult.allChecksSuccess()) {
                                    hasPrivilegeProfiles.add(profileUid);
                                }
                            }, checkPrivilegesException -> {
                                logger.debug(() -> "Failed to check privileges for profile [" + profileUid + "]", checkPrivilegesException);
                                errorProfiles.put(profileUid, checkPrivilegesException);
                            }), () -> {
                                if (counter.decrementAndGet() == 0) {
                                    listener.onResponse(new ProfileHasPrivilegesResponse(hasPrivilegeProfiles, errorProfiles));
                                }
                            })
                        );
                    }
                }), listener::onFailure)
            );
        }, listener::onFailure));
    }

    private void resolveApplicationPrivileges(
        ProfileHasPrivilegesRequest request,
        ActionListener<Collection<ApplicationPrivilegeDescriptor>> listener
    ) {
        final Set<String> applications = Arrays.stream(request.privilegesToCheck().application())
            .map(RoleDescriptor.ApplicationResourcePrivileges::getApplication)
            .collect(Collectors.toSet());
        privilegeStore.getPrivileges(applications, null, listener);
    }
}
