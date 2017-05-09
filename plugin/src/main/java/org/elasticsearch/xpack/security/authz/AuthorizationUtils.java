/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authz;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.xpack.security.authc.Authentication;
import org.elasticsearch.xpack.security.authz.permission.Role;
import org.elasticsearch.xpack.security.support.Automatons;
import org.elasticsearch.xpack.security.user.SystemUser;

import java.util.function.BiConsumer;
import java.util.function.Predicate;

public final class AuthorizationUtils {

    private static final Predicate<String> INTERNAL_PREDICATE = Automatons.predicate("internal:*");

    private AuthorizationUtils() {}

    /**
     * This method is used to determine if a request should be executed as the system user, even if the request already
     * has a user associated with it.
     *
     * In order for the user to be replaced by the system user one of the following conditions must be true:
     *
     * <ul>
     *     <li>the action is an internal action and no user is associated with the request</li>
     *     <li>the action is an internal action and the thread context contains a non-internal action as the originating action</li>
     * </ul>
     *
     * @param threadContext the {@link ThreadContext} that contains the headers and context associated with the request
     * @param action the action name that is being executed
     * @return true if the system user should be used to execute a request
     */
    public static boolean shouldReplaceUserWithSystem(ThreadContext threadContext, String action) {
        if (threadContext.isSystemContext() == false && isInternalAction(action) == false) {
            return false;
        }

        Authentication authentication = threadContext.getTransient(Authentication.AUTHENTICATION_KEY);
        if (authentication == null) {
            return true;
        }

        // we have a internal action being executed by a user that is not the system user, lets verify that there is a
        // originating action that is not a internal action
        final String originatingAction = threadContext.getTransient(AuthorizationService.ORIGINATING_ACTION_KEY);
        if (originatingAction != null && isInternalAction(originatingAction) == false) {
            return true;
        }

        // either there was no originating action or it was a internal action, we should not replace under these circumstances
        return false;
    }

    private static boolean isInternalAction(String action) {
        return INTERNAL_PREDICATE.test(action);
    }

    /**
     * A base class to authorize authorize a given {@link Authentication} against it's users or run-as users roles.
     * This class fetches the roles for the users asynchronously and then authenticates the in the callback.
     */
    public static class AsyncAuthorizer {

        private final ActionListener listener;
        private final BiConsumer<Role, Role> consumer;
        private final Authentication authentication;
        private volatile Role userRoles;
        private volatile Role runAsRoles;
        private CountDown countDown = new CountDown(2); // we expect only two responses!!

        public AsyncAuthorizer(Authentication authentication, ActionListener listener, BiConsumer<Role, Role> consumer) {
            this.consumer = consumer;
            this.listener = listener;
            this.authentication = authentication;
        }

        public void authorize(AuthorizationService service) {
            if (SystemUser.is(authentication.getUser().authenticatedUser())) {
                assert authentication.getUser().isRunAs() == false;
                setUserRoles(null); // we can inform the listener immediately - nothing to fetch for us on system user
                setRunAsRoles(null);
            } else {
                service.roles(authentication.getUser().authenticatedUser(), ActionListener.wrap(this::setUserRoles, listener::onFailure));
                if (authentication.getUser().isRunAs()) {
                    service.roles(authentication.getUser(), ActionListener.wrap(this::setRunAsRoles, listener::onFailure));
                } else {
                    setRunAsRoles(null);
                }
            }
        }

        private void setUserRoles(Role roles) {
            this.userRoles = roles;
            maybeRun();
        }

        private void setRunAsRoles(Role roles) {
            this.runAsRoles = roles;
            maybeRun();
        }

        private void maybeRun() {
            if (countDown.countDown()) {
                try {
                    consumer.accept(userRoles, runAsRoles);
                } catch (Exception e) {
                    listener.onFailure(e);
                }
            }
        }

    }
}
