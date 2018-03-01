/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common;

import java.security.AccessControlContext;
import java.security.Permission;
import org.elasticsearch.SpecialPermission;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

/**
 * This class wraps the operations requiring access in
 * {@link AccessController#doPrivileged(PrivilegedAction)} blocks.
 */
public class SocketAccess {

    protected SocketAccess() {}

    public static <T> T doPrivileged(PrivilegedAction<T> action, AccessControlContext ctx, Permission... perms) {
        SpecialPermission.check();
        return AccessController.doPrivileged(action,ctx, perms);
    }

    public static <T> T doPrivileged(PrivilegedAction<T> action, Permission... perms) {
        if (perms == null || perms.length == 0) {
            SpecialPermission.check();
            return AccessController.doPrivileged(action);
        } else {
            return doPrivileged(action, AccessController.getContext(), perms);
        }
    }

    private static void doPrivilegedVoid(Runnable action, AccessControlContext ctx, Permission... perms) {
        doPrivileged((PrivilegedAction<Void>) () -> {
            action.run();
            return null;
            },
            ctx,
            perms
        );
    }

    public static void doPrivilegedVoid(Runnable action, Permission... perms) {
        if (perms == null || perms.length == 0) {
            SpecialPermission.check();
            // TODO: Code redundance. Can we make this more elegant?
            doPrivileged((PrivilegedAction<Void>) () -> {
                action.run();
                return null;
            });
        } else {
            doPrivilegedVoid(action, AccessController.getContext(), perms);
        }
    }

    /**
     * Do privileged action which throws a checked IOException
     *
     * By using the IO Runnable we ensure that any action that is supplied to this method
     * throws only IOException and nothing else
     * @param action Action to be run. can only throw checked IOException
     * @param ctx AccessControllerContext
     * @param perms
     * @param <T>
     * @return T
     * @throws E
     */
    @SuppressWarnings("unchecked")
    public static <T, E extends Exception> T doPrivilegedException(
        CheckedRunnableWithReturn<T, E> action,
        AccessControlContext ctx,
        Permission... perms) throws E {
        SpecialPermission.check();
        try {
            return AccessController.doPrivileged((PrivilegedExceptionAction<T>) action::run, ctx, perms);
        } catch (PrivilegedActionException e) {
            // Since we use an CheckedRunnable the explicitly thrown exception can only be E
            throw (E) e.getCause();
        }
    }

    @SuppressWarnings("unchecked")
    public static <T, E extends  Exception> T doPrivilegedException(
        CheckedRunnableWithReturn<T, E> action,
        Permission... perms) throws E {
        try {
            if (perms == null || perms.length == 0) {
                SpecialPermission.check();
                return AccessController.doPrivileged((PrivilegedExceptionAction<T>) action::run);
            } else {
                return doPrivilegedException(
                    action,
                    AccessController.getContext(),
                    perms);
            }
        } catch (PrivilegedActionException e) {
            // Since we use a CheckedRunnable the thrown exception can only be E
            throw (E) e.getCause();
        }
    }

    /**
     * Same as {@link #doPrivilegedException} but returns void
     * @param action
     * @throws E
     */
    public static <E extends Exception> void doPrivilegedVoidException(
        CheckedRunnable<E> action,
        AccessControlContext ctx,
        Permission... perms) throws E {
        doPrivilegedException(() -> {
            action.run();
            return null;
        }, ctx, perms);
    }

    /**
     * Same as {@link #doPrivilegedException} but returns void
     * @param action
     * @throws E
     */
    public static <E extends Exception> void doPrivilegedVoidException(
        CheckedRunnable<E> action,
        Permission... perms) throws E {
        if (perms == null || perms.length == 0) {
            doPrivilegedException(() -> {
                action.run();
                return null;
            });
        } else {
            doPrivilegedVoidException(action, AccessController.getContext(), perms);
        }
    }

}
