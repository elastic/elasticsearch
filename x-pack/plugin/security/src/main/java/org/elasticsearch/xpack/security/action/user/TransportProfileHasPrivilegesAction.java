/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.action.user;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

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
        final AuthorizationEngine.PrivilegesToCheck privilegesToCheck = new AuthorizationEngine.PrivilegesToCheck(
            Arrays.asList(request.clusterPrivileges()),
            Arrays.asList(request.indexPrivileges()),
            Arrays.asList(request.applicationPrivileges())
        );
        resolveApplicationPrivileges(request, ActionListener.wrap(applicationPrivilegeDescriptors -> {
            profileService.getProfileSubjects(Arrays.asList(request.profileUids()), ActionListener.wrap(profileSubjectsAndFailures -> {
                threadPool.generic().execute(() -> {
                    final List<String> hasPrivilegeProfiles = Collections.synchronizedList(new ArrayList<>());
                    final List<String> errorProfiles = Collections.synchronizedList(new ArrayList<>(profileSubjectsAndFailures.v2()));
                    final Runnable allDone = () -> listener.onResponse(
                        new ProfileHasPrivilegesResponse(
                            hasPrivilegeProfiles.toArray(new String[0]),
                            errorProfiles.toArray(new String[0])
                        )
                    );
                    final Collection<Map.Entry<String, Subject>> profileUidAndSubjects = profileSubjectsAndFailures.v1().entrySet();
                    final AtomicInteger counter = new AtomicInteger(profileUidAndSubjects.size());
                    if (counter.get() == 0) {
                        allDone.run();
                        return;
                    }
                    for (Map.Entry<String, Subject> profileUidToSubject : profileUidAndSubjects) {
                        final String profileUid = profileUidToSubject.getKey();
                        final Subject subject = profileUidToSubject.getValue();
                        authorizationService.checkPrivileges(
                            subject,
                            privilegesToCheck,
                            applicationPrivilegeDescriptors,
                            ActionListener.wrap(privilegesCheckResult -> {
                                if (privilegesCheckResult.allMatch()) {
                                    hasPrivilegeProfiles.add(profileUid);
                                }
                                if (counter.decrementAndGet() == 0) {
                                    allDone.run();
                                }
                            }, checkPrivilegesException -> {
                                logger.debug(
                                    new ParameterizedMessage("Failed to check privileges for profile [{}]", profileUid),
                                    checkPrivilegesException
                                );
                                errorProfiles.add(profileUid);
                                if (counter.decrementAndGet() == 0) {
                                    allDone.run();
                                }
                            })
                        );
                    }
                });
            }, listener::onFailure));
        }, listener::onFailure));
    }

    private void resolveApplicationPrivileges(
        ProfileHasPrivilegesRequest request,
        ActionListener<Collection<ApplicationPrivilegeDescriptor>> listener
    ) {
        final Set<String> applications = Arrays.stream(request.applicationPrivileges())
            .map(RoleDescriptor.ApplicationResourcePrivileges::getApplication)
            .collect(Collectors.toSet());
        privilegeStore.getPrivileges(applications, null, listener);
    }
}
