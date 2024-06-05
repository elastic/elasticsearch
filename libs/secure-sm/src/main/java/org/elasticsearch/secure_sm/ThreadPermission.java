/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.secure_sm;

import java.security.BasicPermission;

/**
 * Permission to modify threads or thread groups normally not accessible
 * to the current thread.
 * <p>
 * {@link SecureSM} enforces ThreadGroup security: threads with
 * {@code RuntimePermission("modifyThread")} or {@code RuntimePermission("modifyThreadGroup")}
 * are only allowed to modify their current thread group or an ancestor of that group.
 * <p>
 * In some cases (e.g. test runners), code needs to manipulate arbitrary threads,
 * so this Permission provides for that: the targets {@code modifyArbitraryThread} and
 * {@code modifyArbitraryThreadGroup} allow a thread blanket access to any group.
 *
 * @see ThreadGroup
 * @see SecureSM
 */
public final class ThreadPermission extends BasicPermission {

    /**
     * Creates a new ThreadPermission object.
     *
     * @param name target name
     */
    public ThreadPermission(String name) {
        super(name);
    }

    /**
     * Creates a new ThreadPermission object.
     * This constructor exists for use by the {@code Policy} object to instantiate new Permission objects.
     *
     * @param name target name
     * @param actions ignored
     */
    public ThreadPermission(String name, String actions) {
        super(name, actions);
    }
}
